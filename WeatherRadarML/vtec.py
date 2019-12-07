'''
National Weather Service (NWS) Valid Time Event Code (VTEC)
lookup tables for parsing data from Iowa State University
Iowa Environmanetal Mesonet (IEM) NWS Watch/Warnings shapefiles

See following link for more information about VTEC codes:
    https://www.weather.gov/vtec/

See following link for more information about IEM shapefiles:
    https://mesonet.agron.iastate.edu/info/datasets/vtec.html
'''

# Dictionary of phenomenon 2 char codes
# Note that name give long name of the waring and color gives
# the color to display waring as using any method accepted by 
# matplotlib
PHENOM = {
    'AF' : {'name' : 'Ashfall (land)',                  'color' : None}, 
    'AS' : {'name' : 'Air Stagnation',                  'color' : None},
    'BH' : {'name' : 'Beach Hazard',                    'color' : None},
    'BW' : {'name' : 'Brisk Wind',                      'color' : None},
    'BZ' : {'name' : 'Blizzard',                        'color' : None},
    'CF' : {'name' : 'Coastal Flood',                   'color' : None},
    'DF' : {'name' : 'Debris Flow',                     'color' : None},
    'DS' : {'name' : 'Dust Storm',                      'color' : None},
    'DU' : {'name' : 'Blowing Dust',                    'color' : None},
    'EC' : {'name' : 'Extreme Cold',                    'color' : None},
    'EH' : {'name' : 'Excessive Heat',                  'color' : None},
    'EW' : {'name' : 'Extreme Wind',                    'color' : None},
    'FA' : {'name' : 'Areal Flood',                     'color' : None},
    'FF' : {'name' : 'Flash Flood',                     'color' : 'green'},
    'FG' : {'name' : 'Dense Fog (land)',                'color' : None},
    'FL' : {'name' : 'Flood',                           'color' : None},
    'FR' : {'name' : 'Frost',                           'color' : None},
    'FW' : {'name' : 'Fire Weather',                    'color' : None},
    'FZ' : {'name' : 'Freeze',                          'color' : None},
    'GL' : {'name' : 'Gale',                            'color' : None},
    'HF' : {'name' : 'Hurricane Force Wind',            'color' : None},
    'HT' : {'name' : 'Heat',                            'color' : None},
    'HU' : {'name' : 'Hurricane',                       'color' : None},
    'HW' : {'name' : 'High Wind',                       'color' : None},
    'HY' : {'name' : 'Hydrologic',                      'color' : None},
    'HZ' : {'name' : 'Hard Freeze',                     'color' : None},
    'IS' : {'name' : 'Ice Storm',                       'color' : None},
    'LE' : {'name' : 'Lake Effect Snow',                'color' : None},
    'LO' : {'name' : 'Low Water',                       'color' : None},
    'LS' : {'name' : 'Lakeshore Flood',                 'color' : None},
    'LW' : {'name' : 'Lake Wind',                       'color' : None},
    'MA' : {'name' : 'Marine',                          'color' : 'orange'},
    'MF' : {'name' : 'Dense Fog (marine)',              'color' : None},
    'MH' : {'name' : 'Ashfall (marine)',                'color' : None},
    'MS' : {'name' : 'Dense Smoke (marine)',            'color' : None},
    'RB' : {'name' : 'Small Craft for Rough Bar',       'color' : None},
    'RP' : {'name' : 'Rip Current Risk',                'color' : None},
    'SC' : {'name' : 'Small Craft',                     'color' : None},
    'SE' : {'name' : 'Hazardous Seas',                  'color' : None},
    'SI' : {'name' : 'Small Craft for Winds',           'color' : None},
    'SM' : {'name' : 'Dense Smoke (land)',              'color' : None},
    'SR' : {'name' : 'Storm',                           'color' : None},
    'SS' : {'name' : 'Storm Surge',                     'color' : None},
    'SQ' : {'name' : 'Snow Squall',                     'color' : None},
    'SU' : {'name' : 'High Surf',                       'color' : None},
    'SV' : {'name' : 'Severe Thunderstorm',             'color' : 'gold'},
    'SW' : {'name' : 'Small Craft for Hazardous Seas',  'color' : None},
    'TO' : {'name' : 'Tornado',                         'color' : 'red'},
    'TR' : {'name' : 'Tropical Storm',                  'color' : None},
    'TS' : {'name' : 'Tsunami',                         'color' : None},
    'TY' : {'name' : 'Typhoon',                         'color' : None},
    'UP' : {'name' : 'Heavy Freezing Spray',            'color' : None},
    'WC' : {'name' : 'Wind Chill',                      'color' : None},
    'WI' : {'name' : 'Wind',                            'color' : None},
    'WS' : {'name' : 'Winter Storm',                    'color' : None},
    'WW' : {'name' : 'Winter Weather',                  'color' : None},
    'ZF' : {'name' : 'Freezing Fog',                    'color' : None},
    'ZR' : {'name' : 'Freezing Rain',                   'color' : None},
    'ZY' : {'name' : 'Freezing Spray',                  'color' : None}
}

# SIG is 1 char code for significance
SIG = {
        'W' : 'Warning',
        'A' : 'Watch',
        'Y' : 'Advisory',
        'S' : 'Statement',
        'F' : 'Forecast',
        'O' : 'Outlook',
        'N' : 'Synopsis'
}

# Storm-based (polygon) or traditional county/zone based events
GTYPE = {
        'P' : 'Polygon',
        'C' : 'County/zone/parish'
}
