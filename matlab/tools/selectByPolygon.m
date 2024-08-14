function coords = selectByPolygon()
    
    % Load bathymetry data 
    % load('topo.mat', 'topo', 'topomap1');
    % lat = -89.5:1:89.5;
    % lon = -179.5:1:179.5;
    % Z = topo;
    
    % Create world map
    f = figure;
    gx = geoaxes( ...
        'Basemap','None', ...
        'Grid','on' ...
        );
    
    enableDefaultInteractivity(gx);
    geobasemap('satellite');
    geolimits( [-90 90], [-180 180] );
    coords = [];
    
    % Create "Zoom" button
    zoomButton = uicontrol('Style', 'pushbutton', ...
                           'String', 'Zoom', ...
                           'Position', [20, 20, 80, 30], ...
                           'Callback', {@zoomCallback,gx,f} ...
                           );
    
    % Create "Click" button
    clickButton = uicontrol('Style', 'pushbutton', ...
                            'String', 'Click', ...
                            'Position', [120, 20, 80, 30], ...
                            'Callback', {@clickCallback,gx,f} ...
                            );
    
    % Create "Finish" button
    finishButton = uicontrol('Style', 'pushbutton', ...
                            'String', 'Finish', ...
                            'Position', [220, 20, 80, 30], ...
                            'Callback', {@finishCallback} ...
                            );

    oldlat = -1000;
    oldlon = -1000;
    lastline = -1;
    
    waitfor(f);
    
    % Callback for the Click event
    % NB: it disables all gx.Interactions until cleared, unclear why (MATLAB
    % bug?)
    function clickCallback(~, ~, gx, f)
        disp('Click mode activated. Click on the figure to get coordinates.');
        set(f, 'WindowButtonDownFcn', {@mouseClickCallback,gx});
    end
    
    function mouseClickCallback(~, ~, gx)

        [latlim,lonlim] = geolimits();
        % Get the current point when in click mode
        cp = gx.CurrentPoint(1, 1:2); % Get x, y coordinates
        lat = cp(1);
        lon = cp(2);
        coords = [coords; [lat,lon] ];
        fprintf('Adding vertex (%.2f, %.2f)\n', lat, lon);

        geoplot(lat, lon, 'ro'); % Mark the click on the map
        hold on
        if oldlat > -1000
            if ~(lastline==-1)
                delete(lastline(1));
            end
            geoplot([oldlat,lat],[oldlon,lon],'r');
            lastline = geoplot([lat, coords(1,1)],[lon, coords(1,2)],'--r');
        end
        geolimits(latlim,lonlim);

        oldlat = lat;
        oldlon = lon;
        
        enableDefaultInteractivity(gx);
    end
    
    % Re-enable zoom and pan by clearing the click callback
    function zoomCallback(~, ~, ~, f)
        disp('Zoom mode activated.');
        set(f, 'WindowButtonDownFcn', '');
    end

    function finishCallback(~,~)
        coords = [coords; [coords(1,1), coords(1,2)] ];
        close(f);
    end

end