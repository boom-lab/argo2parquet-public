function [coords, radius] = selectPoint()
    
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
    coords = [41.53481,-70.64790];
    
    % User-input radius
    radiusLabel = uicontrol( ...
        'Style', 'text', ...
        'String', 'Enter radius (degrees)', ...
        'Position', [330, 30, 200, 30] ...
        );

    radiusInput = uicontrol( ...
        'Style','edit', ...
        'String', '', ...
        'Position', [380 20 100 20] ...
       );

    radiusUpdate = uicontrol( ...
        'Style','pushbutton', ...
        'String','Update', ...
        'Position', [520, 20, 80, 30], ...
        'Callback', {@updateCallback} ...
        );

    radius = 10;

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
    
    waitfor(f);

    function updateCallback(~,~)

        inputValue = get(radiusInput,'String');
        radius = str2double(inputValue);

        fprintf('Radius = %.2f \n', radius);
        updateDraw(coords);

    end

    function updateDraw(coords)

        lat = coords(1);
        lon = coords(2);

        [latlim,lonlim] = geolimits();

        % Delete previously drawn circles
        % if exist(p1,'var')
        %     delete(p1);
        % end
        % if exist(h1,'var')
        %     delete(r1);
        % end

        % Plotting selected center for radius
        p1 = geoplot(lat, lon, 'ro');
        hold on

        % Plotting area for radius
        theta = linspace(0,2*pi,100);
        lonR = lon + radius*cos(theta);
        latR = lat + radius*sin(theta);

        r1 = geoplot(latR,lonR,'r--');
        geolimits(latlim,lonlim);
        hold off
        
        enableDefaultInteractivity(gx);

    end    
      
    % Callback for the Click event
    % NB: it disables all gx.Interactions until cleared, unclear why (MATLAB
    % bug?)

    function clickCallback(~, ~, gx, f)
        disp('Click mode activated. Click on the figure to get coordinates.');
        set(f, 'WindowButtonDownFcn', {@mouseClickCallback,gx});
    end
    
    function mouseClickCallback(~, ~, gx)

        % Get the current point when in click mode
        cp = gx.CurrentPoint(1, 1:2); % Get x, y coordinates
        lat = cp(1);
        lon = cp(2);
        coords = [lat,lon];
        fprintf('Center: (%.2f, %.2f)\n', lat, lon);
        updateDraw( coords );
        
    end
    
    % Re-enable zoom and pan by clearing the click callback
    function zoomCallback(~, ~, ~, f)
        disp('Zoom mode activated.');
        set(f, 'WindowButtonDownFcn', '');
    end

    function finishCallback(~,~)
        close(f);
    end

end