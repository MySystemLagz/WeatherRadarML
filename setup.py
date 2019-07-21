#!/usr/bin/env python

import setuptools
from disutils.util import convert_path

main_ns  = {}
ver_path = convert_path("WeatherRadarML/version.py")        # Path to version file
with open(ver_path) as ver_file:                            # Open version file for reading
    exec( ver_file.read(), main_ns )                        # Parse data from version file into main_ns

setuptools.setup(
        name                = 'WeatherRadarML',
        description         = 'Improving NEXRAD WSR-88D ZDR relation using machine learning.',
        url                 = 'https://github.com/MySystemLagz/WeatherRadarML',
        author              = 'Allen Dawodu',
        version             = main_ns['__version__'],
        packages            = setuptools.find_packages(),
        install_requires    = ['fiona', 'shapely', 'boto3', 'arm-pyart'],
        package_data        = { '' : ['data/*.txt']},
        zip_save            = False
)

