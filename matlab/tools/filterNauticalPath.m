function data = filterNauticalPath( data, refPoint, distance )

    latP = refPoint(1);
    lonP = refPoint(2);

    boxSW = [ latP-distance, lonP-distance];
    boxNE = [ latP+distance, lonP+distance];

    % numerical index for each row currently present
    indices = linspace(1,height(data),height(data));

    % bool for each row
    data.IN_RADIUS = ones(height(data),1);
    
    for j = 1:height(data)
        floatPoint = [data.LATITUDE(j), data.LONGITUDE(j)];%lat lon of first element
        [path, planner, occWarning] = getPath(refPoint, floatPoint, boxSW, boxNE);
        if occWarning
            floatNb = data.PLATFORM_NUMBER(j);
            time = data.JULD(j);
            lat = data.LATITUDE(j);
            lon = data.LONGITUDE(j);
            warning("Float #" + num2str(floatNb) + ...
                " on " + string(time) + ...
                " at " + num2str(lat) + "," + num2str(lon) + ...
                " is on an occupied spot and is removed. " + ...
                "It is likely that the float is close to the coast " + ...
                "and the occupancy map does not have a good enough resolution." ...
                );            
        end
        if length(path)>1
            delta = pathLength(path);
        else
            delta = 1e6;  % this forces non-existing paths to be discarded later
        end
        if delta > distance
            data.IN_RADIUS(j) = 0;
        end
    end

    % data( indices, : ) = [];

end

function delta = pathLength(path)

    dLat = diff(path(:,1));
    dLon = diff(path(:,2));

    delta = sum( sqrt( dLat.^2 + dLon.^2 ) );

end