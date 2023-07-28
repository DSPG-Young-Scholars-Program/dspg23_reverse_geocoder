import argparse

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from geopy.extra.rate_limiter import RateLimiter
from geopy.point import Point

import pandas as pd
import geopandas as gpd
import numpy as np


#Reverse geocoder tool
def reverse_geocode_tool(df):
    #Creates Geo column: combines latitude and longitude 
    df["Geo"] = df["latitude"].astype(str)+ ',' + df["longitude"].astype(str)
    
    geolocator = Nominatim(user_agent="reverse_geocoder", timeout=10)
    rgeocode = RateLimiter(geolocator.reverse, min_delay_seconds=0.001)
    
    #Reverse geocodes 'Geo' column and populates 'address' column with results
    df['address'] = df['Geo'].apply(rgeocode)
    
    #returns the reverse-geocoded dataframe
    return df


def main():
    #Creates argument parser
    parser = argparse.ArgumentParser(description="Reverse Geocoder")

    #'cf' should be a path to a csv file that contains 'latitude' and 'longitude' columns
    parser.add_argument('--cf', type=str, required=True, help="Path to data file.")

    #'sf' is a path to a shp file that corresponds with the county in 'county_data'
    parser.add_argument('--sf', type=str, required=True, help="Path to shape file.")

    #'of' is a path to a csv file that contains the output dataframe
    parser.add_argument('--of', type=str, required=True, help="Path to output file")
    
    args = parser.parse_args()

    file_path = args.cf
    shape_file_path = args.sf
    output_path = args.of

    county_data = pd.read_csv(file_path)
    county_shape = gpd.read_file(shape_file_path)

    #Prints number of missing addresses in 'county_data'
    print("No. of missing Addresses:", sum(county_data['address'].isna()))

    #'Missing_data': df containing all the rows of 'county_data' that have missing values in 'address' columns
    missing_data = county_data[county_data['address'].isna()]

    #Creates 'centroid' column based on 'geometry' - gives us latitude and longitude values
    county_shape['centroid'] = county_shape['geometry'].centroid

    #Converts 'GEOID20' column to int, so it can be merged with 'missing_data'
    county_shape['GEOID20']=county_shape['GEOID20'].astype(int)

    #Merges 'missing_data' with 'county_shape' on 'GEOID20' col (left join) and drops unnecesary columns from 'merged_df'
    merged_df = missing_data.merge(county_shape, on='GEOID20', how="left")
    merged_df = merged_df.drop(['STATEFP20', 'COUNTYFP20', 'TRACTCE20', 'BLOCKCE20', 'NAME20', 'MTFCC20', 'UR20', 'UACE20', 'UATYPE20', 'FUNCSTAT20', 'ALAND20', 'AWATER20', 'INTPTLAT20', 'INTPTLON20', 'geometry'], axis=1)

    #Poulates 'latitude' and 'longitude' cols of 'merged_df' with lat and long vals from 'centroid'
    for i in merged_df.index:   
        point = merged_df['centroid'][i]
        lat = point.y
        long = point.x
        merged_df['latitude'][i] = lat
        merged_df['longitude'][i] = long
    
    #Reverse geocodes 'merged_df'
    reverse_geocode_tool(merged_df)

    #Drops 'centroid' and 'Geo' columns from 'merged_df'
    merged_df = merged_df.drop(['centroid', 'Geo'], axis=1)

    #Checks if 'address' column in 'county_data' is NaN
    #if so, updates 'address', 'latitude', and 'longitude' columns of 'county_data' 
    #with 'address', 'latitude', and 'longitude' columns of 'merged_df', based on 'GEOID20'
    for i in county_data.index:
        if pd.isna(county_data['address'][i]):  
            county_data['address'][i] = merged_df['address'][merged_df.loc[merged_df['GEOID20']==county_data['GEOID20'][i]].index[0]]
            county_data['latitude'][i] = merged_df['latitude'][merged_df.loc[merged_df['GEOID20']==county_data['GEOID20'][i]].index[0]]
            county_data['longitude'][i] = merged_df['longitude'][merged_df.loc[merged_df['GEOID20']==county_data['GEOID20'][i]].index[0]]

    #Saves output df as csv and prints the number of missing address in the output dataframe
    county_data.to_csv(output_path)
    print("No. of missing Addresses:", sum(county_data['address'].isna()))
    print("Reverse geocoding complete!")
    

if __name__ == '__main__':
    main()