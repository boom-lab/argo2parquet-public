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
import multiprocessing

import sys
import itertools
from itertools import islice

if sys.version_info >= (3, 12):
    from itertools import batched
else:
    try:
        from more_itertools import batched
    except ImportError:
        def batched(iterable, chunk_size):
            iterator = iter(iterable)
            while chunk := tuple(islice(iterator, chunk_size)):
                yield chunk

root = '.'
# Function to download and parse GDAC synthetic profile index file
def argo_gdac(gdac_path='./', dataset="bgc", lat_range=None,lon_range=None,start_date=None,end_date=None,sensors=None,floats=None,overwrite_profiles=False,skip_downloads=True,download_individual_profs=False,save_to=None,verbose=True,dryrun=False,dac_url_root=None,checktime=True, NPROC=1):
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
          checktime: download files from repository only if they are newer than files
                    on disk (overwrite flag is neglected if true)
          dac_url_root: root directory to download/copy data from
          NPROC: number of processors to use to donwload argo files

    """

    if dataset=="bgc":
        gdac_name = 'argo_synthetic-profile_index.txt'
    elif dataset=="phy":
        gdac_name = 'ar_index_global_prof.txt'
    else:
        raise ValueError('Dataset variable must be set to bgc or phy.')

    if not os.path.exists(gdac_path + gdac_name):
        print(gdac_name + ' not found in ' + gdac_path + '. Downloading it.')
        gdac_url  = 'https://usgodae.org/pub/outgoing/argo/'
        download_file(gdac_url,gdac_name,save_to=gdac_path,overwrite=True,verbose=verbose,checktime=checktime)


  # Load index file into Pandas DataFrame
    gdac_file = gdac_path+gdac_name
    gdac_index = pd.read_csv(gdac_file,delimiter=',',header=8,parse_dates=['date','date_update'],
                             date_parser=lambda x: pd.to_datetime(x,format='%Y%m%d%H%M%S'))

  # Establish time and space criteria
    if lat_range is None:  lat_range = [-90.0,90.0]
    if lon_range is None:  lon_range = [-180.0,180.0]
    elif lon_range[0] > 180 or lon_range[1] > 180:
        if lon_range[0] > 180: lon_range[0] -= 360
        if lon_range[1] > 180: lon_range[1] -= 360
    if start_date is None: start_date = datetime(1900,1,1)
    if end_date is None:   end_date = datetime(2200,1,1)

    # file name convention
    # [institution ('aoml', 'coriolis', etc)] / [wmo_id] / profiles / [real time ('R') or delayed ('D') mode + wmo_id + cycle number + eventual descending profile ('D')] + .nc
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
        if not dataset=='bgc': ValueError('sensors can only be used with bgc dataset')
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
                                  save_to=save_to,overwrite=overwrite_profiles,verbose=verbose,checktime=checktime)
        else:
            urls = []
            localpaths = []
            local_fnames = []
            downloaded_paths = []
            if dataset=='bgc':
                prof_ext = '_Sprof.nc'
            elif dataset=='phy':
                prof_ext = '_prof.nc'
                
            for f_idx, wmoid_filepath in enumerate(wmoid_filepaths):
                filename = str(wmoids[f_idx]) + prof_ext
                downloaded_filenames.append( filename )
                urls.append( dac_url_root + wmoid_filepath )
                localpath = Path(save_to,wmoid_filepath)
                localpath.mkdir(parents= True, exist_ok= True)
                localpaths.append( localpath )
                local_filename = str(localpath) + '/' + filename
                local_fnames.append(local_filename)

            if not dryrun: # it still returns the filename that would be downloaded

                if NPROC == 1:
                    for url_path, filename, localpath  in zip(urls, downloaded_filenames, localpaths):
                        download_file(url_path,filename,save_to=str(localpath)+'/',
                                      overwrite=overwrite_profiles,
                                      verbose=verbose,checktime=checktime)

                else:
                    nb_to_download = len(downloaded_filenames)

                    if NPROC > 100:
                        print('Limiting to 100 processors.')
                        NPROC = 100
                    if NPROC > nb_to_download:
                        NPROC = nb_to_download
                        print('More processors than files requested, limiting NPROC to number of files.')

                    CHUNK_SZ = int(np.ceil(nb_to_download/NPROC))
                    chunks_fname = batched(downloaded_filenames,CHUNK_SZ)
                    chunks_url = batched(urls,CHUNK_SZ)
                    chunks_saveto = batched(localpaths,CHUNK_SZ)
                    args_download = [(chunk_url[0], chunk_fname[0], save_to[0], overwrite_profiles, verbose, checktime) for chunk_url, chunk_fname, chunk_saveto in zip(chunks_url, chunks_fname, chunks_saveto) ]
                    pool_obj = multiprocessing.Pool(processes=NPROC)
                    pool_obj.map( download_file_mp, args_download )
                    pool_obj.close()
                    pool_obj.join()


        if (not dryrun) and verbose: print("All requested files have been downloaded.")

        return wmoids, gdac_index_subset, local_fnames

    else:
        return wmoids, gdac_index_subset

# download all individual profiles in df
def download_profiles(df,gdac_root='https://www.usgodae.org/ftp/outgoing/argo/',local_root='./gdac',overwrite=False,verbose=True,checktime=True):
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
                      save_to=str(localpath)+ '/',overwrite=overwrite,verbose=verbose,checktime=checktime)
        downloaded_filenames.append(filename)
    return downloaded_filenames

# Request url
def get_func(url,stream=True):
    try:
        return requests.get(url,stream=stream,auth=None,verify=False)
    except requests.exceptions.ConnectionError as error_tag:
        print('Error connecting:',error_tag)
        time.sleep(1)
        return get_func(url,stream=stream)

# Get url file modification time
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
        checktime: downloads file from url_path if they are newer than file on disk
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

        if os.path.exists(localfile):
            if checktime:
                current_file_time = datetime.fromtimestamp(os.path.getmtime(localfile))
                new_file_time = get_time_url(url_path + filename)
                tz = new_file_time.tzinfo
                current_file_time = current_file_time.replace(tzinfo=tz).astimezone(tz)
                if not new_file_time > current_file_time:
                    if verbose: print('>>> File ' + filename + ' at requested URL (' + str(url_path) + ') is not newer than file on disk and is not downloaded.')
                    return

            elif not overwrite:
                if verbose: print('>>> File ' + filename + ' already exists. Leaving current version.')
                return
            else:
                if verbose: print('>>> File ' + filename + ' already exists. Overwriting with new version.')


        response = get_func(url_path + filename,stream=True)

        if response.status_code == 404:
            if verbose: print('>>> File ' + filename + ' returned 404 error during download (requested URL: ' + str(url_path) + ').')
            return
        with open(save_to+filename,'wb') as out_file:
            shutil.copyfileobj(response.raw,out_file)
            del response

        if verbose: print('>>> Successfully downloaded ' + filename + '.')

    except:
        if verbose: print('>>> An error occurred while trying to download ' + filename + ' from ' + url_path + '.')

# Function to download a single file, in parallel
def download_file_mp(args):

    url_path,filename,save_to,overwrite,verbose,checktime = args

    """Downloads and saves a file from a given URL using HTTP protocol.

    Note: If '404 file not found' error returned, function will return without downloading anything.
    
    Arguments:
        url_path: root URL to download from including trailing slash ('/')
        filename: filename to download including suffix
        save_to: None (to download to root Google Drive GO-BGC directory)
                 or directory path
        overwrite: False to leave existing files in place
                   or True to overwrite existing files (neglected if checktime is true)
        checktime: downloads file from url_path if they are newer than file on disk
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

        if os.path.exists(localfile):
            if checktime:
                current_file_time = datetime.fromtimestamp(os.path.getmtime(localfile))
                new_file_time = get_time_url(url_path + filename)
                tz = new_file_time.tzinfo
                current_file_time = current_file_time.replace(tzinfo=tz).astimezone(tz)
                if not new_file_time > current_file_time:
                    if verbose: print('>>> File ' + filename + ' at requested URL (' + str(url_path) + ') is not newer than file on disk and is not downloaded.')
                    return

            elif not overwrite:
                if verbose: print('>>> File ' + filename + ' already exists. Leaving current version.')
                return
            else:
                if verbose: print('>>> File ' + filename + ' already exists. Overwriting with new version.')

        response = get_func(url_path + filename,stream=True)

        if response.status_code == 404:
            if verbose: print('>>> File ' + filename + ' returned 404 error during download (requested URL: ' + str(url_path) + ').')
            return
        with open(localfile,'wb') as out_file:
            shutil.copyfileobj(response.raw,out_file)
            del response
        if verbose: print('>>> Successfully downloaded ' + filename + '.')

    except:
        if verbose: print('>>> An error occurred while trying to download ' + filename + ' from ' + url_path + '.')
