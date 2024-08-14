function scatterMap(data,varName,plotTitle)

% Plot scatter map of varName
%
% data: table containing (at least) latitude, longitude, and varName
% varName: name of the variable to plot (char or string array)
%
% Note: if a point (lat0,lon0) appears more than once, the relative varName
% entries are averaged
%
% Examples:
%
% scatterMap(data,'TEMP_ADJUSTED')
% creates and displays map with scattered plot of delayed temperature data

    % check that (lat0,lon0) are unique, otherwise average data
    [G, LAT, LON] = findgroups(data.LATITUDE,data.LONGITUDE);
    meanVar = splitapply(@mean, data.(varName), G);

    TID = table( ...
        LAT, LON, meanVar, ...
        'VariableNames', {'LATITUDE', 'LONGITUDE', varName} ...
        );
    
    % initialize figure with basemap
    deltaLAT = max(LAT) - min(LAT);
    deltaLON = max(LON) - min(LON);
    latlim = [min(LAT)-0.2*deltaLAT, max(LAT)+0.2*deltaLAT];
    lonlim = [min(LON)-0.2*deltaLON, max(LON)+0.2*deltaLON];

    f = figure;
    gx = geoaxes( ...
        'Basemap','None', ...
        'Grid','on' ...
        );
    
    % enableDefaultInteractivity(gx);
    geobasemap('satellite');
    geolimits( latlim, lonlim );
    
    geoscatter(...
        TID.LATITUDE, ...
        TID.LONGITUDE, ...
        60, ...
        TID.(varName), ...
        'filled' ...
        );

    % cosmetics
    colormap("copper")
    colorbar;
    title(plotTitle);

end