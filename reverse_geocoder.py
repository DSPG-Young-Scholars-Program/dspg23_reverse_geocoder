#Import libraries

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from geopy.extra.rate_limiter import RateLimiter
from geopy.point import Point

import pandas as pd
import geopandas as gpd
import numpy as np


#reads in National Address Database data for the Bronx (36005) - 'bronx_data'
file_path = '../dspg23_reverse_geocoder/Data/36005.csv'
bronx_data = pd.read_csv(file_path)


#Creates Reverse geocoder tool
#Input: df w/ a 'latitude' and 'longitude' column, Output: df with updated 'address' column
def reverse_geocode_tool(df):
    #Creates Geo column: combines latitude and longitude 
    df["Geo"] = df["latitude"].astype(str)+ ',' + df["longitude"].astype(str)
    
    geolocator = Nominatim(user_agent="reverse_geocoder", timeout=10)
    
    rgeocode = RateLimiter(geolocator.reverse, min_delay_seconds=0.001)
    
    #Reverse geocodes 'Geo' column and populates 'address' column with results
    df['address'] = df['Geo'].apply(rgeocode)
    
    return df


#'Missing_data': df containing all the rows of 'bronx_data' that have missing values in 'address' columns
missing_data = bronx_data[bronx_data['address'].isna()]


#Reads in shape file - 'bronx_shape'
shape_file_path = '../dspg23_reverse_geocoder/Data/tl_2020_36005_tabblock20/tl_2020_36005_tabblock20.shp'
bronx_shape = gpd.read_file(shape_file_path)

#Creates 'centroid' column based on 'geometry' - gives us latitude and longitude values
bronx_shape['centroid'] = bronx_shape['geometry'].centroid

#Converts 'GEOID20' column to int, so it can be merged with 'missing_data'
bronx_shape['GEOID20']=bronx_shape['GEOID20'].astype(int)


#Merges 'missing_data' with 'bronx_shape' on 'GEOID20' col (left join)
merged_df = missing_data.merge(bronx_shape, on='GEOID20', how="left")
merged_df = merged_df.drop(['STATEFP20', 'COUNTYFP20', 'TRACTCE20', 'BLOCKCE20', 'NAME20', 'MTFCC20', 'UR20', 'UACE20', 'UATYPE20', 'FUNCSTAT20', 'ALAND20', 'AWATER20', 'INTPTLAT20', 'INTPTLON20', 'geometry'], axis=1)

#Poulates 'latitude' and 'longitude' cols of 'merged_df' with lat and long vals from 'centroid'
for i in merged_df.index:   
    point = merged_df['centroid'][i]
    lat = point.y
    long = point.x
    
    merged_df['latitude'][i] = lat
    merged_df['longitude'][i] = long
    
merged_df = merged_df.drop('centroid', axis=1)

#Reverse geocodes 'merged_df'
reverse_geocode_tool(merged_df)
merged_df = merged_df.drop('Geo', axis=1)


#Checks if 'address' column in 'bronx_data' is NaN
#if so, updates 'address', 'latitude', and 'longitude' columns of 'bronx_data' 
#with 'address', 'latitude', and 'longitude' columns of 'test_data', based on 'GEOID20'
for i in bronx_data.index:
    if pd.isna(bronx_data['address'][i]):  
        bronx_data['address'][i] = merged_df['address'][merged_df.loc[merged_df['GEOID20']==bronx_data['GEOID20'][i]].index[0]]
        bronx_data['latitude'][i] = merged_df['latitude'][merged_df.loc[merged_df['GEOID20']==bronx_data['GEOID20'][i]].index[0]]
        bronx_data['longitude'][i] = merged_df['longitude'][merged_df.loc[merged_df['GEOID20']==bronx_data['GEOID20'][i]].index[0]]


#Saves output df as csv
bronx_data.to_csv('../dspg23_reverse_geocoder/36005_updated.csv')

print(bronx_data)
