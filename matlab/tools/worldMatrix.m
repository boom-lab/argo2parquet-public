clear all;

% Loading coastlines
S = shaperead("./GSHHG/GSHHS_shp/i/GSHHS_i_L1.shp");
precision = 1e2;
S = shapesByArea(S,precision);
% plotCoastlines(S,precision);

% Creating empty world grid at 0.01 degree resolution
    resolution = 1;%0.01; % degrees
worldLat0 =  -90;
worldLat1 =   90;
worldLon0 = -180;
worldLon1 =  180;
latGrid = worldLat0:resolution:worldLat1;
lonGrid = worldLon0:resolution:worldLon1;
latPoints = latGrid(1:end-1)+resolution/2;
lonPoints = lonGrid(1:end-1)+resolution/2;
nbLat = length(latPoints);
nbLon = length(lonPoints);
% empty (false) grid
grid = false(nbLat,nbLon);

gridVector = zeros(nbLat*nbLon,2);

% tic
% parfor l=1:nbLat*nbLon
%     k = mod(l,nbLon) + 1 ;
%     j = idivide( int32(l), nbLon) + int32(mod(l,nbLon)~=0); % adding one if condition satisfied
% 
%     lat = latPoints(j);
%     lon = lonPoints(k);
%     for s = 1:length(S)
%         shape = S(s);
%         landPolygonLON = shape.X(1:end-1); % discarding final NaN
%         landPolygonLAT = shape.Y(1:end-1); % discarding final NaN
%         if inpolygon( ...
%                 lat, ...
%                 lon, ...
%                 landPolygonLAT, ...
%                 landPolygonLON ...
%                 )
%             gridVector(l,:) = [j,k];
%             break
%         end
%     end
% end
% gridVector = gridVector( ~all(gridVector==0,2), :);
% elapsed = toc;
% disp("Elapsed time to create it vector format: " + num2str(elapsed) + " seconds.")

tic
% Fill grid where land is, i.e. if grid points are inside any polygon
%
% Note 1: grid is "upside-down", i.e. north points south, because
% binaryOccupancyMap indexes the map with the top-left corner being (1,1)
% and bottom-right being (nbRows,nbCols), so it would flip it later. I 
% already build the map upside-down so that no extra memory is needed to
% flip it.
%
% Note 2: it seems that a full matrix with only bool types is smaller than a
% sparse matrix that tracks the same info
parfor j = 1:nbLat
    for k = 1:nbLon
        lat = latPoints(nbLat+1-j); %flipping rows for occupancyMap later
        lon = lonPoints(k);
        for s = 1:length(S)
            shape = S(s);
            landPolygonLON = shape.X(1:end-1); % discarding final NaN
            landPolygonLAT = shape.Y(1:end-1); % discarding final NaN
            if inpolygon( ...
                    lat, ...
                    lon, ...
                    landPolygonLAT, ...
                    landPolygonLON ...
                    )
                grid(j,k) = true;
                break
            end
        end
    end
end
elapsed = toc;
disp("Elapsed time to create it matrix format: " + num2str(elapsed) + " seconds.")

occupancyMap = binaryOccupancyMap(grid);

fname = './worldOccupancyMap.mat';
save(fname,'grid','occupancyMap','-v7.3');


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

function plotCoastlines(S,precision)
    figure;
    for s=1:length(S)
        shape = S(s);
        if shape.area > precision
            plot(shape.X(1:end-1), shape.Y(1:end-1));
            hold on;
        end
    end
end

function S = shapesByArea(S,precision)
    for s=length(S):-1:1
            shape = S(s);
            if shape.area < precision
                S(s)=[];
            end
    end
end