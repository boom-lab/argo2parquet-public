%%% Plotting temperature measurements in the North-West Atlantic
%
% This examples shows how to read and manipulate Argo data stored in
% parquet format. We will filter the data by depth (pressure), time, and
% geographical location. We will then compute the average value of the
% temperature for each float, and plot it.

% Clear all variables and add relative path
clear all
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
    'PRES_ADJUSTED',...
    'TEMP_ADJUSTED'...
    };
    
% Set up the reader. This returns a ParquetDatastore object of the database
% (no need to care about what a ParquetDatastore object exactly is for
% now).
% The targetDatabase argument is required; selectVariables can be omitted,
% in which case all the parameters are read.
pds = setUpReader(targetDatabase, selectVariables);

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

% Filtering geographical coordinates for NWA region
% Note: we pass the filter we just generated to append this filter to it
filter = filterBy('LATITUDE',34,80,filter); 
filter = filterBy('LONGITUDE',-78,-50,filter); 

% Applying filters to the database
pds = applyFilter(pds,filter);

% Reading the data into memory, serially (timing the operation)
tic;
dataSerial = readall(pds,UseParallel=false);
elapsed = toc;
disp("Elapsed time to read data into memory serially: " + num2str(elapsed) + " seconds.")

% Reading the data into memory, in parallel (timing the operation)
p = gcp('nocreate');
if isempty(p)
    parpool; % you can specificy the number of workers with 
             % parpool(nbWorkers); the default shoud be fine
end
tic;
dataParallel = readall(pds,UseParallel=true);
elapsed = toc;
disp("Elapsed time to read data into memory in parallel: " + num2str(elapsed) + " seconds.")