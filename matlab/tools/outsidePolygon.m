function indices = outsidePolygon(table, polygon)

    inPolygon = inpolygon( ...
        table.LATITUDE, ...
        table.LONGITUDE, ...
        polygon(:,1), ...
        polygon(:,2) ...
        ); 

    % numerical index for each row currently present
    indices = linspace(1,height(table),height(table));

    if length(inPolygon) ~= length(indices)
        error('Table height and indices vector length do not match.')
    end

    % indices of rows of point outside polygon and to remove
    indices = indices( inPolygon==false );

end