clear all;

latP = 19;
lonP = -110;
refPoint = [latP,lonP];

latE = 19;
lonE = -83; %purposefully out of bound
floatPoint = [latE,lonE];

boxSW = [  0, -120];
boxNE = [ 30,  -70];

[path, planner] = getPath(refPoint, floatPoint, boxSW, boxNE);