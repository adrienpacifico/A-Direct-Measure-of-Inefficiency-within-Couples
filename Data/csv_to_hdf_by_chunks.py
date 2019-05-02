# -*- coding: utf-8 -*-
"""


@author: IMPTEMP_A_PACIFIC
"""
import datetime
from os import listdir
from os.path import isfile, join
import pandas as pd


hdf_path = './Data/hdf/edp_2015_final.h5'
tables_path = './Data/csv/'


files = [f for f in listdir(tables_path) if isfile(join(tables_path,f))]

i = 0
for file in files:
    print file
    key = file.replace(" ", "")
    if file[-4:] == '.CSV':
        reader = pd.read_csv(join(tables_path,file), chunksize=5*10**2)#5*10**6)
        j=0        
        for chunk in reader:
            chunk.to_hdf(hdf_path, 
                         key.replace("EDP_BE2015_", "").replace(".CSV",""),
                          )
            print("chunk: {}".format( j))
            j+=1
        del reader
    else:
        print file, "is not a sas file "
    i+=1
    print '*'*40
    print i,'/',len(files), 'completed at: \n {} '.format( datetime.datetime.now())
	
#
#table = SAS7BDAT(table_path)
#table_df = table.to_data_frame()
#df.to_hdf(hdf_path, table_name, complevel = 9 , complib = 'zlib')
 
Store = pd.HDFStore(hdf_path)



































