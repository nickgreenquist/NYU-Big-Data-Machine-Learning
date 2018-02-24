'''
('505233', '501414')
('505238', '503996')
('500081', '505233')
('500008', '500009')
('500004', '500005')
'''

import csv
import os
import sys
import math
import pickle
import pandas as pd
import numpy as np

keys = [('505233', '501414'), ('505238', '503996'), ('500081', '505233'),('500008', '500009'), ('500004', '500005')]
data = os.getcwd() + '/data'
for key in keys:
    file_out = open(str(key[0]) + '_' + str(key[1]) + '.txt', 'ab')
    for filename in os.listdir(data):
        obj = pd.read_pickle(os.path.join(data, filename))
        temp = obj.get(key)
        values = temp.reset_index().values
        np.savetxt(file_out, values, fmt=['%.0f', '%f'], delimiter=',')
    file_out.close()
