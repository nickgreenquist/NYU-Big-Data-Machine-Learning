import os
import sys
import math
import numpy as np
import pandas as pd
import pickle

best = {}

filenames = [
    '/data/2014-08-01-segments-speeds-avged.pickle', 
    '/data/2014-08-02-segments-speeds-avged.pickle',
    '/data/2014-08-03-segments-speeds-avged.pickle',
    '/data/2014-08-04-segments-speeds-avged.pickle',
    '/data/2014-08-05-segments-speeds-avged.pickle',
    '/data/2014-08-06-segments-speeds-avged.pickle',
    '/data/2014-08-07-segments-speeds-avged.pickle',
]
#obj = pd.read_pickle(os.getcwd() + '/data/2014-08-01-segments-speeds-avged.pickle', None)
for filename in filenames:
    f = open(os.getcwd() + filename, 'rb') # opening pickle file, use ".pickle.2" if you are using Python 2
    obj = pickle.load(f)
    f.close()
    for k, v in obj.items():
        goodValues = 0
        for index, row in v.iterrows():
            if not math.isnan(row['AvgSpeed']):
                goodValues += 1
        if k in best:
            best[k] += goodValues
        else:
            best[k] = goodValues

output = sorted(best, key=best.get, reverse=True)
output = output[:20]
for out in output:
    print(out)

#Best 20 timeseries from one week
'''
('505233', '501414')
('505238', '503996')
('500081', '505233')
('500008', '500009')
('500004', '500005')
('501414', '500197')
('500084', '503195')
('500197', '500084')
('500009', '500010')
('503967', '500367')
('503195', '501419')
('500005', '505284')
('500367', '500368')
('302434', '306533')
('500369', '500370')
('500368', '500369')
('500255', '503967')
('500370', '500371')
('504462', '500428')
('500371', '500372')
'''