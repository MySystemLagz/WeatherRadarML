from shapely.geometry import MultiPolygon, Polygon
from datetime import datetime
from matplotlib.colors import to_rgba
import numpy as np
from . import vtec

class wwaVTEC( MultiPolygon ):
    def __init__(self, record):
        '''
        Purpose:
            To initialize the wwaVTEC class
        Inputs:
            record : A record from an IEM shapefile containing NWS WWa VTEC
                      records
        '''
        coords = record['geometry']['coordinates']                              # Coordinages for polygon(s)
        if (record['geometry']['type'] == 'Polygon'):                           # If type is Polygon
            super().__init__( [ Polygon( coords[0] ) ] )                        # Use coordinates to create Polygon in list and then initiailze super class (MulitPolygon) with list of single Polygon
        else:                                                                   # Else, assume type is MultiPolygon
            super().__init__( [ Polygon( coord[0] ) for coord in coords ] )     # Iterate over all coordinates, initializing Polygon for each coordinate; all Polygons go in list; initialize super class
 
        for key, val in record['properties'].items():                           # Iterate over key/value pairs in record properties
            if (key == 'ISSUED') or (key == 'EXPIRED'):                         # If key is 'ISSUED' or 'EXPIRED'
                setattr(self, key, datetime.strptime(val, '%Y%m%d%H%M'))        # Parse date into datetime object and set class attribute using key 
            else:                                                               # Else, want to just copy attributes
                if (key == 'TYPE'): key = 'PHENOM'                              # If key is TYPE, set to PHENOM for consistency
                table = getattr(vtec, key, None)                                # Attempt to get key from vtec table
                if table:                                                       # If table found in vtec table
                    setattr(self, key, table[val])                              # Set class attribute with name key to value from vtec table
                else:                                                           # Else
                    setattr(self, key, val)                                     # Set class attribute with name key to value val

    @property
    def xy(self):
        '''
        Purpose:
            Property to return x,y values of all polygons
        Example:
            x, y = inst.xy
        '''
        xx = []                                                                 # Initialize list for x values
        yy = []                                                                 # Initialize list for y values 
        for poly in self:                                                       # Iterate over all polygons
            x, y = poly.exterior.xy                                             # Get x, y values for polygon
            xx.append(x)                                                        # Append x values to xx
            yy.append(y)                                                        # Append y values to yy
        return xx, yy                                                           # Return the x and y values

    def plot(self, axes, **kwargs): 
        '''
        Purpose:
            Method to plot all polygons in instance on a
            maplotlib.pyplot.axes object
        Inputs:
            axes  : The axes object to plot polygon(s) on
        Keywords:
                        All keywords accepted by matplotlib.pyplot.fill
            Default behavior for some select keywords below:
                linewidth : Sets the width of the edge line.
                            Default is 2
                edgecolor : Sets to color of the edge.
                            Default is to use the color defined
                            in the phenomenon
                facecolor : Sets the face (fill) color.
                            Default is to use the edgecolor but
                            with saturation set to 0.075
        Output:
            List of maplotlib.patches.Polgon objects 
        '''
        if ('linewidth' not in kwargs):                                         # If linewidth not in kwargs
            kwargs['linewidth'] = 2                                             # Add to kwargs with value of 2

        if ('edgecolor' not in kwargs):                                         # If edgecolor not in kwargs
            kwargs['edgecolor'] = to_rgba( self.PHENOM['color'] )               # Set edgecolor to RGBA of phenomenon color
        elif isinstance(kwargs['edgecolor'], str):                              # Else, if the edgecolor value is type string, then convert to RGBA
            kwargs['edgecolor'] = to_rgba( kwargs['edgecolor'] )                # Convert edgecolor to RGBA
        
        if ('facecolor' not in kwargs):                                         # If facecolor not in kwargs
            kwargs['facecolor'] = kwargs['edgecolor']                           # Set facecolor to edgecolor
        elif isinstance(kwargs['facecolor'], str):                              # Else, if the edgecolor value is type string, then convert to RGB
            kwargs['facecolor'] = to_rgba( kwargs['facecolor'] )                # Convert edgecolor to RGBA

        kwargs['facecolor']     = list(kwargs['facecolor'])                     # Convert facecolor to list
        kwargs['facecolor'][-1] = 0.1                                           # Set alpha channel for facecolor to 0.1; i.e., 90% transparent

        objs = []                                                               # List to store all matplotlib Polygons returned from .fill()
        for x, y in zip(*self.xy):                                              # Get x, y value lists, zip them up, iterate over them 
            objs += axes.fill(x, y, **kwargs)                                   # Plot each polygon on the axes. All fill instances appended objs list
        return objs                                                             # Return list of matplotlib Polygon objects

