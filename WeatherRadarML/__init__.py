import os

os.environ['PYART_QUIET'] = '1'			# Supress pyart banner on package import

dataDir = os.path.join( os.path.dirname( __file__ ), 'data' )
