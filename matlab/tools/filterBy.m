function updatedFilter = filterBy(varName,minValue,maxValue,varargin)

% Update filter to apply when reading parquet database.
%
% varName: name of the variable to filter (char or string array)
% minValue: minimum value for filter; if maxValue is empty, it is
%           the value that the filter is set equal to
% maxValue: maximum value for filter; if empty the filter sets the variable
%           equals to minValue
% varargin: cells containing previously defined filters (optional); if not
%           provided, previoulsy defined filters are ignored and a new one 
%           is generated
%
% updatedFilter: cells containing updated list of filters
%
%
% Examples:
%
% filterPres = filterBy('PRES_ADJUSTED',0,50)
% creates and applies filter to read only entries where the adjusted
% pressure is between 0 and 50 dbar.
%
% filterPresAndNb = filterBy('PLATFORM_NUMBER',5902519,[],filterPres)
% further requires to retrieve data only for the float numbered 5902519.

    % validate inputs
    append = false;
    if minValue > maxValue
        error('Minimum value larger than maximum value.')
    elseif nargin > 5
        error('Too many input arguments, maximum is five.')
    elseif nargin < 3
        error('Not enough input arguments, minimum four are needed.')
    elseif nargin == 4
        oldFilter = varargin{1};
        if ~iscell(oldFilter)
            error('Input filter is not a cell.');
        elseif ~size(oldFilter, 2) == 3
            error('Input filter cell must have three columns.');
        end
        append = true;
    end

    newFilter = {varName, minValue, maxValue};

    % update filters cell
    if append
        updatedFilter = [oldFilter;newFilter];
    else
        updatedFilter = newFilter;
    end

end

function filter = generateFilter(rf,conditions)

% Generate filters compatible with parquet datastore grammar
%
% rf: RowFilter object of the database
% conditions: 1x3 cell containing the variable name, minimum and maximum
%             values for the filter
%
% filter: filter applicable to parquet datastore object

    if ~iscell(conditions)
        error('Input filter is not a cell.');
    elseif ~size(conditions, 2) == 3
        error('Input filter cell must have three columns.');
    elseif ~size(conditions, 1) == 1
        error('Input filter cell must have one row.');
    end

    varName = conditions{1,1};
    minValue = conditions{1,2};
    maxValue = conditions{1,3};
    % if maxValue is empty, treat filter as an equality condition
    if isempty(maxValue)
        filter = rf.(varName) == minValue;
    else
        filter = rf.(varName) >= minValue & rf.(varName) <= maxValue ;
    end

end

