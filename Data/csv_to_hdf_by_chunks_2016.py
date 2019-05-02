# -*- coding: utf-8 -*-
"""


@author: IMPTEMP_A_PACIFIC
"""
import datetime
from os import listdir
from os.path import isfile, join
import pandas as pd


#hdf_path = 'C:/Users/IMPTEMP_A_PACIFIC/Desktop/EDP/EDP_HDF/edp4.h5'
#table_path = 'C:/Users/IMPTEMP_A_PACIFIC/Desktop/EDP/EDP 2014/EDP_BE2014_EAR2004_INDIVIDU.sas7bdat'
#table_name = 'EDP_BE2014_EAR2004_INDIVIDU'

hdf_path = 'C:/Users/IMPTEMP_A_PACIFIC/Desktop/EDP_2016/Data/hdf/edp_2016_final.h5'
tables_path = 'C:/Users/IMPTEMP_A_PACIFIC/Desktop/EDP_2016/Data/csv/'


files = [f for f in listdir(tables_path) if isfile(join(tables_path,f))]
#files  = ["EDP_BE2014_FISC_INDIVIDU        .CSV"]

i = 0
for file in files:
#for file in ['EDP_BE2014_FISC_REVENU.sas7bdat']:
    print(file)
    key = file.replace(" ", "")
    if file[-4:] == '.CSV':
        reader = pd.read_csv(join(tables_path,file), chunksize=5*10**9, #Huge chunk size,try 10^6 for small configurations
                             encoding = "latin1")#5*10**6)              #, warning mix dtypes issues could arrise
        j=0        
        for chunk in reader:
            chunk.to_hdf(hdf_path, 
                         key.replace("EDP_BE2016_", "").replace(".CSV",""),
                          )
            print(j)
            j+=1
        del reader
    else:
        print(file, "is not a sas file ")
    i+=1
    print('*'*40)
    print(i,'/',len(files), 'completed at: \n {} '.format( datetime.datetime.now()))
	
#
#table = SAS7BDAT(table_path)
#table_df = table.to_data_frame()
#df.to_hdf(hdf_path, table_name, complevel = 9 , complib = 'zlib')
 
Store = pd.HDFStore(hdf_path)