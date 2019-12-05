import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
def baseMap( linewidth  = 0.5, 
             resolution = '50m', 
             extent     = (-130, -60, 25, 50),
             projection = 'PlateCarree' ):
    '''
    Purpose:
        Function to set up base map using cartopy for plotting data
    Inputs:
        None.
    Keywords:
        linewidth  : Sets width of coastlines, state boarders, etc.
        resolution : Sets resolution for coastlines, state boarders, etc.
        extent     : Sets map limit; (lonMin, lonMax, latMin, latMax)
        projection : Name of projection to use
    Outputs:
        Returns a matplotlib axes object
    '''
    ax = plt.axes( projection= getattr(ccrs, projection)() )
    ax.set_extent( extent )
    ax.coastlines(linewidth = linewidth, resolution = resolution)
    ax.add_feature( cfeature.STATES.with_scale(resolution), linewidth=linewidth)
    return ax

def plotStation( ax, name, coords, color = 'g' ):
    '''
    Purpose:
        Function to plot an ASOS station
    Inputs:
        ax     : Axis object to plot station on
        name   : Name of the station
        coords : [X, Y] coordinates of the station
    Keywords:
        color  : Color to use for station dot
    Outputs:
        None.
    '''
    ax.scatter( *coords, color = color )
    ax.annotate( name, coords )

def plotWarning( ax, record ):
    xy = np.asarray( record['geometry']['coordinates'] ).squeeze()
    ax.plot( xy[:,0], xy[:,1], '-', color = color )
    ax.fill( xy[:,0], xy[:,1], color = color )

if __name__ == "__main__":
    ax = baseMap()
    plt.show()
