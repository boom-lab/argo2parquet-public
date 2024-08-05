function pds = setUpReader(argoDatabase, varargin)

% Generate ParquetDatastore object of the database accounting for
% restrictions on variables to read and type of Argo database desired
%
% argoDatabase: Argo database, valid values are "PHY" for the Core Argo
%               physical parameters, and "BGC" for the biogeochemical 
%               variables.
% selectVariables: cells listing Argo parameters to read; it requires each
%                  entry to be a character array (not a string array), i.e.
%                  {'PRES_ADJUSTED', 'PLATFORM_NUMBER'} is valid, but 
%                  {"PRES_ADJUSTED",% "PLATFORM_NUMBER"} is not (note the 
%                  double quotation marks).
%
% pds: ParquetDatastore object of the database

    if strcmp(argoDatabase,"PHY")
        pqtPath = fullfile('.','database','ArgoPHY');
    elseif strcmp(argoDatabase,"BGC")
        pqtPath = fullfile('.','database','ArgoBGC');
    else
        error("argoDatabase can only take values 'PHY' or 'BGC', invalid value provided");
    end
    location = matlab.io.datastore.FileSet(pqtPath); % faster parsing

    selVar = false;
    if nargin == 2
        selectVariables = varargin{1};
        selVar = true;
    end

    if selVar
        pds = parquetDatastore(...
            location, ...
            "FileExtensions",".parquet", ...
            "IncludeSubfolders", false, ...
            "OutputType", "table", ...    
            "VariableNamingRule","preserve", ...
            "SelectedVariableNames", selectVariables ...
        );
    else
        pds = parquetDatastore(...
        location, ...
        "FileExtensions",".parquet", ...
        "IncludeSubfolders", false, ...
        "OutputType", "table", ...    
        "VariableNamingRule","preserve" ...
        );
    end

end

