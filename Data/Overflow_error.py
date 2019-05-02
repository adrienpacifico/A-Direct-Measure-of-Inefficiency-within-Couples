# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 10:53:02 2017

@author: IMPTEMP_A_PACIFIC
"""

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

hdf_path = 'C:/Users/IMPTEMP_A_PACIFIC/Desktop/EDP_2015/Data/hdf/edp_2015_final.h5'
tables_path = 'C:/Users/IMPTEMP_A_PACIFIC/Desktop/EDP_2015/Data/csv/'


i = 0
for file in [join(tables_path,'EDP_BE2015_FISC_INDIVIDU        .CSV')]:
#for file in ['EDP_BE2014_FISC_REVENU.sas7bdat']:
    print file
    key = file.replace(" ", "")
    if file[-4:] == '.CSV':
        reader = pd.read_csv(join(tables_path,file), chunksize=5*10**6)#Reduce depending of available ram 5*10**6)
        j=0        
        for chunk in reader:
            chunk.to_hdf(hdf_path, 
                         key.replace("EDP_BE2015_", "").replace(".CSV",""),
                          )
            print "chunk number {}".format(j)
            j+=1
        del reader
    else:
        print file, "is not a sas file "
    i+=1
    print '*'*40
    #print i,'/',len(files), 'completed at: \n {} '.format( datetime.datetime.now())
	
#
#table = SAS7BDAT(table_path)
#table_df = table.to_data_frame()
#df.to_hdf(hdf_path, table_name, complevel = 9 , complib = 'zlib')
 
Store = pd.HDFStore(hdf_path)

