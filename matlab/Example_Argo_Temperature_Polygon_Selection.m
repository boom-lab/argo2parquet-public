%% ===================================================================== %%
% Plotting temperature measurements in the North-West Atlantic
% ======================================================================= %
%
% This examples shows how to read and manipulate Argo data stored in
% parquet format. We will filter the data by depth (pressure), time, and
% geographical location. We will then compute the average value of the
% temperature for each float, and plot it.

%% Setup
% Clear all variables and add relative path
clear all; close all;
addpath( fullfile('.','tools') );

% State what Argo database you want to access: 'PHY' for Argo Core, 'BCG'
% for the biogeochemical Argo.
targetDatabase = "PHY"; % "BGC"

% Create a cell listing the Argo parameters to read; it requires each
% entry to be a character array (not a string array), i.e.
% {'PRES_ADJUSTED', 'PLATFORM_NUMBER'} is valid, but {"PRES_ADJUSTED",
% "PLATFORM_NUMBER"} is not (note the double quotation marks).
% Note that it must contains the variables to which filters are later
% applied.
selectVariables = {...
    'LATITUDE',...
    'LONGITUDE',...
    'JULD',...
    'PLATFORM_NUMBER',...
    'PRES_ADJUSTED',...
    'TEMP_ADJUSTED'...
    };
    
% Set up the reader. This returns a ParquetDatastore object of the database
% (no need to care about what a ParquetDatastore object exactly is for
% now).
% The targetDatabase argument is required; selectVariables can be omitted,
% in which case all the parameters are read.
pds = setUpReader(targetDatabase, selectVariables);

%% Filtering
% We now create the filters for our data,. The syntax is:
% filterby(varName, minValue [, maxValue, filter])
% such that: minValue <= varName <= maxValue
% or: varName == minValue (if maxValue == [])
% with:
% varName: the name of the parameter we are filtering; it takes standard
%          Argo names (e.g. PSAL, DOXY_ADJUSTED);
% minValue: minimum value for filter; if maxValue is empty, it is
%           the value that the filter is set equal to;
% maxValue: maximum value for filter; if empty (i.e. equals to []) the 
%           filter sets the variable equals to minValue.
% filter: a filter previously generated with this same command to which we
%         want to append more conditions; we need to generate one condition
%         at the time.

% Filtering for measurements performed at 50m depth (i.e. 50 dbar)
filter = filterBy('PRES_ADJUSTED',50,[]);

% Filtering geographical coordinates with interactive polygon selection
% Note: this needs to be done in a few stages. i) a polygon is drawn; ii)
% all the data the fits in the smallest square that includes the polygon
% are loaded (parquet loading cannot use ad hoc filters); iii) data outside
% of polygon is discarded.
% i) selection
polygonCoords = selectByPolygon();
minLat = min(polygonCoords(:,1));
maxLat = max(polygonCoords(:,1));
minLon = min(polygonCoords(:,2));
maxLon = max(polygonCoords(:,2));
% ii) first filter (box)
filter = filterBy('LATITUDE',minLat,maxLat,filter); 
filter = filterBy('LONGITUDE',minLon,maxLon,filter);

% Filtering by date and time, e.g. if we want to look at the data between
% 12pm on October 1st, 2023 and 9:15pm on April 30th, 2024
startTime = datetime(2023,10,1,12,0,0); % year, month, day, hour (24h format), min, sec
endTime   = datetime(2024,4,30,21,15,0); % year, month, day, hour (24h format), min, sec
filter = filterBy('JULD',startTime,endTime,filter);

% Applying filters to the database
pds = applyFilter(pds,filter);

%% Reading the data into memory, serially (timing the operation)
% tic;
% dataSerial = readall(pds,UseParallel=false);
% elapsed = toc;
% disp("Elapsed time to read data into memory serially: " + num2str(elapsed) + " seconds.")

%% Reading the data into memory, in parallel (timing the operation)
p = gcp('nocreate');
if isempty(p)
    tic
    parpool; % you can specificy the number of workers with 
             % parpool(nbWorkers); the default shoud be fine
    elapsed = toc;
    disp("Elapsed time to  create parallel environment: " + num2str(elapsed) + " seconds.")
end
tic;
dataParallel = readall(pds,UseParallel=true);
elapsed = toc;
disp("Elapsed time to read data into memory in parallel: " + num2str(elapsed) + " seconds.")

rowsToRemove = outsidePolygon(dataParallel,polygonCoords);
dataParallel( rowsToRemove, : ) = [];

if height(dataParallel) < 1
    error('No float matches all the filters you provided: are your filters too restrictive?');
end

%% Plotting target data
% Now we can make a scatter plot of the temperature data recorded
scatterMap(dataParallel, 'TEMP_ADJUSTED', 'Temperature in NWA at 50 dbar')

%% Basic statistics
% We can also quickly investigate some statistics of the loaded data
dataStats = summary(dataParallel);
% and for example see the min, max, and median values of the temperatue
disp("Minimum temperature = " + num2str(dataStats.TEMP_ADJUSTED.Min));
disp("Maximum temperature = " + num2str(dataStats.TEMP_ADJUSTED.Max));
disp("Median temperature = " + num2str(dataStats.TEMP_ADJUSTED.Median));