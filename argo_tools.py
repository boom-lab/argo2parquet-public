from datetime import datetime, timedelta
from dateutil.parser import parse as parsedate
import requests
import time
import os
import pathlib
from pathlib import Path
import urllib3
import shutil
import numpy as np
import pandas as pd
from scipy import interpolate
import xarray as xr
from multiprocessing.pool import ThreadPool

root = '.'
# Function to download and parse GDAC synthetic profile index file
def argo_gdac(gdac_path='./argo_synthetic-profile_index.txt',lat_range=None,lon_range=None,start_date=None,end_date=None,sensors=None,floats=None,overwrite_profiles=False,skip_downloads=True,download_individual_profs=False,save_to=None,verbose=True,dryrun=False,dac_url_root=None):
    """Downloads GDAC Sprof index file, then selects float profiles based on criteria.
      Either returns information on profiles and floats (if skip_downloads=True) or downloads them (if False).

      Arguments:
          lat_range: None, to select all latitudes
                     or [lower, upper] within -90 to 90 (selection is inclusive)
          lon_range: None, to select all longitudes
                     or [lower, upper] within either -180 to 180 or 0 to 360 (selection is inclusive)
                     NOTE: longitude range is allowed to cross -180/180 or 0/360
          start_date: None or datetime object
          end_date:   None or datetime object
          sensors: None, to select profiles with any combination of sensors
                   or string or list of strings to specify required sensors
                   > note that common options include PRES, TEMP, PSAL, DOXY, CHLA, BBP700,
                                                      PH_IN_SITU_TOTAL, and NITRATE
          floats: None, to select any floats matching other criteria
                  or int or list of ints specifying floats' WMOID numbers
          overwrite_index: False to keep existing downloaded GDAC index file, or True to download new index
          overwrite_profiles: False to keep existing downloaded profile files, or True to download new files
          skip_downloads: True to skip download and return: (<list of WMOIDs>, <DataFrame of index file subset>,
                                                            <list of downloaded filenames [if applicable]>)
                         or False to download those profiles
          download_individual_profs: False to download single Sprof file containing all profiles for each float
                                     or True to download individual profile files for each float
          save_to: None to download to Google Drive "/GO-BGC Workshop/Profiles" directory
                   or string to specify directory path for profile downloads
          verbose: True to announce progress, or False to stay silent
          dryrun: If True, returns list of filenames that would be downloaded, without
                  downloading them (note that it requires skip_downloads=False)
          dac_url_root: root directory to download/copy data from

    """
    gdac_index = pd.read_csv(gdac_path,delimiter=',',header=8,parse_dates=['date','date_update'],
                             date_parser=lambda x: pd.to_datetime(x,format='%Y%m%d%H%M%S'))


  # Load index file into Pandas DataFrame
  

  # Establish time and space criteria
    if lat_range is None:  lat_range = [-90.0,90.0]
    if lon_range is None:  lon_range = [-180.0,180.0]
    elif lon_range[0] > 180 or lon_range[1] > 180:
        if lon_range[0] > 180: lon_range[0] -= 360
        if lon_range[1] > 180: lon_range[1] -= 360
    if start_date is None: start_date = datetime(1900,1,1)
    if end_date is None:   end_date = datetime(2200,1,1)

    float_wmoid_regexp = r'[a-z]*/[0-9]*/profiles/[A-Z]*([0-9]*)_[0-9]*[A-Z]*.nc'
    gdac_index['wmoid'] = gdac_index['file'].str.extract(float_wmoid_regexp).astype(int)
    filepath_main_regexp = '([a-z]*/[0-9]*/)profiles/[A-Z]*[0-9]*_[0-9]*[A-Z]*.nc'
    gdac_index['filepath_main'] = gdac_index['file'].str.extract(filepath_main_regexp)
    filepath_regexp = '([a-z]*/[0-9]*/profiles/)[A-Z]*[0-9]*_[0-9]*[A-Z]*.nc'
    gdac_index['filepath'] = gdac_index['file'].str.extract(filepath_regexp)
    filename_regexp = '[a-z]*/[0-9]*/profiles/([A-Z]*[0-9]*_[0-9]*[A-Z]*.nc)'
    gdac_index['filename'] = gdac_index['file'].str.extract(filename_regexp)
    cycle_regexp = '[a-z]*/[0-9]*/profiles/[A-Z]*[0-9]*_([0-9]*)[A-Z]*.nc'
    gdac_index['cycle'] = gdac_index['file'].str.extract(cycle_regexp).astype(int)

    # Subset profiles based on time and space criteria
    gdac_index_subset = gdac_index.loc[np.logical_and.reduce([gdac_index['latitude'] >= lat_range[0],
                                                              gdac_index['latitude'] <= lat_range[1],
                                                              gdac_index['date'] >= start_date,
                                                              gdac_index['date'] <= end_date]),:]
    if lon_range[1] >= lon_range[0]:    # range does not cross -180/180 or 0/360
        gdac_index_subset = gdac_index_subset.loc[np.logical_and(gdac_index_subset['longitude'] >= lon_range[0],
                                                                 gdac_index_subset['longitude'] <= lon_range[1])]
    elif lon_range[1] < lon_range[0]:   # range crosses -180/180 or 0/360
        gdac_index_subset = gdac_index_subset.loc[np.logical_or(gdac_index_subset['longitude'] >= lon_range[0],
                                                                gdac_index_subset['longitude'] <= lon_range[1])]

    # If requested, subset profiles using float WMOID criteria
    if floats is not None:
        if type(floats) is not list: floats = [floats]
        gdac_index_subset = gdac_index_subset.loc[gdac_index_subset['wmoid'].isin(floats),:]

    # If requested, subset profiles using sensor criteria
    if sensors is not None:
        if type(sensors) is not list: sensors = [sensors]
        for sensor in sensors:
            gdac_index_subset = gdac_index_subset.loc[gdac_index_subset['parameters'].str.contains(sensor),:]

    # Examine subsetted profiles
    wmoids = gdac_index_subset['wmoid'].unique()
    wmoid_filepaths = gdac_index_subset['filepath_main'].unique()

    # Just return list of floats and DataFrame with subset of index file, or download each profile
    if not skip_downloads:
        downloaded_filenames = []
        if dac_url_root is None:
            dac_url_root = 'https://usgodae.org/pub/outgoing/argo/dac/'

        if download_individual_profs:
            for p_idx in gdac_index_subset.index:
                filename = gdac_index_subset.loc[p_idx]['filename']
                downloaded_filenames.append(filename)
                if not dryrun: # it still returns the filename that would be downloaded
                    filepath = os.path.join('dac',gdac_index_subset.loc[p_idx]['filepath'])
                    localpath = Path('./gdac',filepath)
                    localpath.mkdir(parents= True, exist_ok= True)
                    download_file(dac_url_root + gdac_index_subset.loc[p_idx]['filepath'],
                                  filename,
                                  save_to=save_to,overwrite=overwrite_profiles,verbose=verbose)
        else:
            for f_idx, wmoid_filepath in enumerate(wmoid_filepaths):
                downloaded_filenames.append(str(wmoids[f_idx]) + '_Sprof.nc')
                if not dryrun: # it still returns the filename that would be downloaded
                    download_file(dac_url_root + wmoid_filepath,str(wmoids[f_idx]) + '_Sprof.nc',
                                  save_to=save_to,overwrite=overwrite_profiles,verbose=verbose)

        if (not dryrun) and verbose: print("All requested files have been downloaded.")

        return wmoids, gdac_index_subset, downloaded_filenames

    else:
        return wmoids, gdac_index_subset

# download all individual profiles in df
def download_profiles(df,gdac_root='https://www.usgodae.org/ftp/outgoing/argo/',local_root='./gdac',overwrite=False,verbose=True):
    """[summary]

    Args:
        df ([type]): [description]
        gdac_root (str, optional): [description]. Defaults to 'https://data-argo.ifremer.fr/'.
        local_root (str, optional): [description]. Defaults to './gdac'.

    Returns:
        [type]: [description]
    """
    downloaded_filenames = []
    for p_idx in df.index:
        filename = df.loc[p_idx]['filename']
        filepath = os.path.join('dac',df.loc[p_idx]['filepath'])
        localpath = Path(local_root,filepath)
        localpath.mkdir(parents= True, exist_ok= True)
        download_file(gdac_root+filepath,filename,
                      save_to=str(localpath)+ '/',overwrite=overwrite,verbose=verbose)
        downloaded_filenames.append(filename)
    return downloaded_filenames



# Function to download a single file
def download_file(url_path,filename,save_to=None,overwrite=False,verbose=True,checktime=True):
    """Downloads and saves a file from a given URL using HTTP protocol.

    Note: If '404 file not found' error returned, function will return without downloading anything.
    
    Arguments:
        url_path: root URL to download from including trailing slash ('/')
        filename: filename to download including suffix
        save_to: None (to download to root Google Drive GO-BGC directory)
                 or directory path
        overwrite: False to leave existing files in place
                   or True to overwrite existing files (neglected if checktime is true)
        checktime: downloads only file at url_path is newer than file on disk
                   (overwrite flag is neglected)
        verbose: True to announce progress
                 or False to stay silent

    """

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if save_to is None:
        save_to = root
    localfile = os.path.join(save_to,filename)
    if verbose: print('>>>> Destination file: ' + str(localfile) + '.')
    try:

        def get_time_url(url):
            try:
                r = requests.head(url)
                url_time = r.headers['last-modified']
                return parsedate(url_time)
            except requests.exceptions.RequestException as e:
                print("Request failed:", e)
            except requests.exceptions.HTTPError as e:
                print("HTTP error occurred:", e)
                print("Status code:", response.status_code)
                print("Response content:", response.text)
            except Exception as e:
                print("Other error occurred:", e)

        if os.path.exists(localfile):
            if checktime:
                current_file_time = datetime.fromtimestamp(os.path.getmtime(localfile))
                new_file_time = get_time_url(url_path + filename)
                tz = new_file_time.tzinfo
                current_file_time = current_file_time.replace(tzinfo=tz).astimezone(tz)
                if not new_file_time > current_file_time:
                    if verbose: print('>>> File ' + filename + ' on disk newer than file at requested URL (' + str(url_path) + ') and is not downloaded.')
                    return

            elif not overwrite:
                if verbose: print('>>> File ' + filename + ' already exists. Leaving current version.')
                return
            else:
                if verbose: print('>>> File ' + filename + ' already exists. Overwriting with new version.')

        def get_func(url,stream=True):
            try:
                return requests.get(url,stream=stream,auth=None,verify=False)
            except requests.exceptions.ConnectionError as error_tag:
                print('Error connecting:',error_tag)
                time.sleep(1)
                return get_func(url,stream=stream)

        response = get_func(url_path + filename,stream=True)

        if response.status_code == 404:
            if verbose: print('>>> File ' + filename + ' returned 404 error during download (requested URL: ' + str(url_path) + ').')
            return
        with open(save_to + filename,'wb') as out_file:
            shutil.copyfileobj(response.raw,out_file)
            del response
        if verbose: print('>>> Successfully downloaded ' + filename + '.')

    except:
        if verbose: print('>>> An error occurred while trying to download ' + filename + '.')
