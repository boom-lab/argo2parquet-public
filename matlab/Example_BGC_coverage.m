%% ===================================================================== %%
% Plotting BGC coverage
% ======================================================================= %
%
% This examples shows how to read and manipulate Argo data stored in
% parquet format. We will filter the data by depth (pressure), time, and
% geographical location. We will then compute the average value of the
% temperature for each float, and plot it.

%% Setup
% Clear all variables and add relative path
clear all
addpath( fullfile(".","tools") );

% State what Argo database you want to access: "PHY" for Argo Core, "BCG"
% for the biogeochemical Argo.
targetDatabase = "BGC";

% Get AJUSTED variables QC names
vars_QC = getQC("BGC");

% Create a cell listing the Argo parameters to read; it requires each
% entry to be a character array (not a string array), i.e.
% {"PRES_ADJUSTED", "PLATFORM_NUMBER"} is valid, but {"PRES_ADJUSTED",
% "PLATFORM_NUMBER"} is not (note the double quotation marks).
% Note that it must contains the variables to which filters are later
% applied.
selectVariables = [...
    "LATITUDE";...
    "LONGITUDE";...
    "JULD";...
    "PLATFORM_NUMBER";...
    "PRES_ADJUSTED";...
    ];
selectVariables = [selectVariables; vars_QC];

    
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

% Filtering for measurements performed at more than 100 dbar
filter = filterBy("PRES_ADJUSTED",100,1e10,{},"AND");

% Filtering by date and time, e.g. if we want to look at the data between
% 12pm on October 1st, 2023 and 9:15pm on April 30th, 2024
startTime = datetime(2023,10,1,12,0,0); % year, month, day, hour (24h format), min, sec
endTime   = datetime(2024,4,30,21,15,0); % year, month, day, hour (24h format), min, sec
filter = filterBy("JULD",startTime,endTime,filter,"AND");

[~, filter] = applyFilter(pds,filter);

% Filtering ADJUSTED_QC equal to 1 or 2
filterQC = filterBy(vars_QC(1),1,2,{},"OR");
for j=2:length(vars_QC)
    filterQC = filterBy(vars_QC(j),1,2,filterQC,"OR");
end
[~, filterQC] = applyFilter(pds,filterQC);

filter = filter & filterQC;

% Applying filters to the database
% pds = applyFilter(pds,filter);
pds.RowFilter = filter;

%% Reading the data into memory, serially (timing the operation)
tic;
dataBGC = readall(pds,UseParallel=false);
elapsed = toc;
disp("Elapsed time to read data into memory serially: " + num2str(elapsed) + " seconds.")

%% Reading the data into memory, in parallel (timing the operation)
% p = gcp("nocreate");
% if isempty(p)
%     tic
%     parpool; % you can specificy the number of workers with 
%              % parpool(nbWorkers); the default shoud be fine
%     elapsed = toc;
%     disp("Elapsed time to  create parallel environment: " + num2str(elapsed) + " seconds.")
% end
% tic;
% dataBGC = readall(pds,UseParallel=true);
% elapsed = toc;
% disp("Elapsed time to read data into memory in parallel: " + num2str(elapsed) + " seconds.")

%% Plotting target data
% Now we can make a scatter plot of the temperature data recorded
scatterMap(dataBGC, "PRES_ADJUSTED", "PRES_ADJUSTED", "auto")

%% Basic statistics
% We can also quickly investigate some statistics of the loaded data
dataStats = summary(dataBGC);
% and for example see the min, max, and median values of the temperatue
disp("Minimum pressure = " + num2str(dataStats.PRES_ADJUSTED.Min));
disp("Maximum pressure = " + num2str(dataStats.PRES_ADJUSTED.Max));
disp("Median pressure = " + num2str(dataStats.PRES_ADJUSTED.Median));
