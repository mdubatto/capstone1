import pyspark as ps
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructType, StructField, ArrayType

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
# !pip install geopy
import folium
from geopy.geocoders import Nominatim
import time
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.5f' % x)

spark = (ps.sql.SparkSession
         .builder
         .master('local[4]')
         .appName('lecture')
         .getOrCreate()
        )
sc = spark.sparkContext

park = spark.read.csv('../data/Parking_Violations_Issued_FY_2019.csv',
                         header=True,
                         inferSchema=True)

park2 = (park
         .drop('Violation In Front Of Or Opposite',
               'Violation Post Code',
               'To Hours In Effect',
               'From Hours In Effect',
               'Intersecting Street',
               'Violation Description',
               'Violation Legal Code',
               'Meter Number',
               'Unregistered Vehicle?',
               'Time First Observed',
               'No Standing or Stopping Violation',
               'Hydrant Violation',
               'Double Parking Violation')
         .na.drop()
        )

park3 = park2.filter((park2['Violation Code'].isin(21,38,14,20,46)))

park_sample = park3.sample(False, 0.01).toPandas()

park_sample['Address'] = park_sample['House Number'] + ' ' + park_sample['Street Name'] + ' NYC'

addr = pd.read_csv('../data/address.csv')

def unpack(row):
    try:
        return {'lat': row['location'][0], 'long': row['location'][1]}
    except:
        return np.nan

coord = pd.concat([addr, addr.apply(unpack, axis=1, result_type='expand')], axis=1)