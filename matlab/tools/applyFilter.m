function pds = applyFilter(pds,conditions)

% Apply filter to ParquetDatastore object of the database
%
% pds: ParquetDatastore object of the database
% filters: Nx3 cells containing updated list of N filters

    % generate filters to apply to parquet datastore compatible with
    % parquet datastore grammar
    rf = rowfilter(pds);
    filter = generateFilter(rf,conditions(1,:));
    conditionsNb = size(conditions, 1);
    if conditionsNb > 1
        for j=2:conditionsNb
            filterToAdd = generateFilter(rf,conditions(j,:));

            % generate combined filter taking into account of all
            % conditions that we have looped over so far
            filter = filter & filterToAdd;
        end
    end

    % apply filters to parquet datastore
    pds.RowFilter = filter;

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

