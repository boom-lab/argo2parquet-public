function [path, lPlanner] = getPath(refPoint, floatPoint, boxSW, boxNE)

    wom = load('./worldOccupancyMap_p1e2_r010.mat');
    
    globalMap = occupancyMap(wom.grid);
    wPlanner = plannerAStarGrid(globalMap);
    
    worldWidth = 360;
    worldHeight = 180;
    mapWidth = length(wom.grid(1,:));
    mapHeight = length(wom.grid(:,1));
    resolution = worldWidth/mapWidth;
    
    [globalStartCol, globalStartRow] = latlon2grid( refPoint, 'point', [-90,-180], resolution, mapHeight);
    [globalEndCol, globalEndRow] = latlon2grid( floatPoint, 'point', [-90,-180], resolution, mapHeight);
    path = wPlanner.plan( ...
        [globalStartRow, globalStartCol], ...
        [globalEndRow, globalEndCol] ...
        );
    
    boxHeight = boxNE(1) - boxSW(1);
    boxWidth = boxNE(2) - boxSW(2);
    
    gridWidth = boxWidth/resolution;
    gridHeight = boxHeight/resolution;
    
    % [gridWidth,gridHeight] = latlon2grid( boxSW, 'box', boxNE, resolution);
    [gridCol, gridRow] = latlon2grid( boxSW, 'point', [-90,-180], resolution, mapHeight);
    localMap = occupancyMap( ...
        wom.grid( ...
        (gridRow-gridHeight):gridRow, ... % still, the axis is downward...
        gridCol:(gridCol+gridWidth) ...
        ) ...
        );
    lPlanner = plannerAStarGrid(localMap);
    [localStartCol, localStartRow] = latlon2grid( refPoint, 'point', boxSW, resolution, gridHeight);
    [localEndCol, localEndRow]     = latlon2grid( floatPoint, 'point', boxSW, resolution, gridHeight);
    localPath = lPlanner.plan( ...
        [localStartRow, localStartCol],...
        [localEndRow, localEndCol]...
        );

end

function [x,y] = latlon2grid( latlon, varargin )

    lat0 = latlon(1);
    lon0 = latlon(2);

    switch varargin{1}

        case 'point'
            % [x,y] are the coordinates of latlon on the target grid
            % to be called as:
            % [x,y] = latlon2grid( [lat0,lon0], 'point', [SWcornerLat, SWcornerLon], resolution)
            
            boxSW = varargin{2};
            boxSWLat = boxSW(1);
            boxSWLon = boxSW(2);

            resolution = varargin{3};
            gridHeight = varargin{4};

            % shifting, rescaling, and rounding to grid resolution
            DeltaY = lat0 - boxSWLat;
            DeltaX = lon0 - boxSWLon;
            x = int32( DeltaX/resolution );
            y = int32( gridHeight - DeltaY/resolution );
        
        case 'box'
        
            % [x,y] are the width and height of the box in the target grid
            point2 = varargin{2};
            resolution = varargin{3};

            lat2 = point2(1);
            lon2 = point2(2);

            latWidth = lat2 - lat0;
            lonWidth = lon2 - lon0;
            if latWidth < 0 || lonWidth < 0
                error(['The first argument must be the SW corner, ' ...
                       'the second the NE corner of the box.'])
            end
            
            gridWidth = lonWidth/resolution;
            gridHeight = latWidth/resolution;

            x = gridWidth;
            y = gridHeight;

    end

end