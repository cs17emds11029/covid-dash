# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 16:17:03 2020

@author: Srinivas
"""

import pandas as pd
import boto3
from io import StringIO

class Covid19:
    #covid_ts_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    #covid_ts_deaths_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
    covid_ts_recovd_global_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'

    def __init__(self):
        #self.covid_ts_global = pd.read_csv(Covid19.covid_ts_global_url)
        #self.covid_ts_deaths_global = pd.read_csv(Covid19.covid_ts_deaths_global_url)
        self.covid_ts_recovd_global = pd.read_csv(Covid19.covid_ts_recovd_global_url)

    '''
    def transform_and_write_global(self):
        # The format of the file is Province/State, Country/Region and a series of dates
        # transform the format into date, Province/State, Country/Region and cumcount
        self.covid_ts_global.drop(labels=['Lat','Long'],axis=1,inplace=True)
        self.covid_ts_global = self.covid_ts_global.pivot_table(columns=['Province/State','Country/Region']).reset_index()
        self.covid_ts_global.columns = ['date','province_state','country_region','cumulative_count']
        
        # generate a new count column
        self.covid_ts_global.sort_values(by=['country_region','province_state','date'])
        self.covid_ts_global['new_count'] = self.covid_ts_global.groupby(['country_region','province_state'])['cumulative_count'].diff().fillna(0)
        
        #self.covid_ts_global.to_csv('D:/Projects/covid-dash/covid_ts_global.csv', index=False)
        self.write_to_s3("covid_ts_global.csv",self.covid_ts_global)
    '''
    def transform_and_write_recovd_global(self,access_key,secret_access):
        # The format of the file is Province/State, Country/Region and a series of dates
        # transform the format into date, Province/State, Country/Region and cumcount
        self.covid_ts_recovd_global.drop(labels=['Lat','Long'],axis=1,inplace=True)
        self.covid_ts_recovd_global['Province/State'].fillna(self.covid_ts_recovd_global['Country/Region'],inplace=True)        
        self.covid_ts_recovd_global = self.covid_ts_recovd_global.pivot_table(columns=['Province/State','Country/Region']).reset_index()
        self.covid_ts_recovd_global.columns = ['date','province_state','country_region','cumulative_count']
        
        # generate a new count column
        self.covid_ts_recovd_global.sort_values(by=['country_region','province_state','date'])
        self.covid_ts_recovd_global['new_count'] = self.covid_ts_recovd_global.groupby(['country_region','province_state'])['cumulative_count'].diff().fillna(0)
        
        #self.covid_ts_global.to_csv('D:/Projects/covid-dash/covid_ts_global.csv', index=False)
        self.write_to_s3("covid_ts_recovd_global.csv",
                         self.covid_ts_recovd_global,
                         access_key,
                         secret_access)

    def write_to_s3(self,filename,data,access_key,secret_access):
        try:
            s3_resource = boto3.resource("s3",
                                         aws_access_key_id=access_key,
                                         aws_secret_access_key=secret_access)
            bucket = "spaturub"
            csv_buffer = StringIO()
            data.to_csv(csv_buffer)
            s3_resource.Object(bucket,filename).put(Body=csv_buffer.getvalue())
            print("Uploading to S3 successfull")
        except:
           print("Uploading to S3 not successfull")
           
def lambda_handler(event, context):
    Covid19obj = Covid19()
    Covid19obj.transform_and_write_recovd_global(event['ACCESS_KEY'],
                                                 event['SECRET_ACCESS'])