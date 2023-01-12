#Import Library
import numpy as np
import pandas as pd
from sqlalchemy import create_engine #write the result to SQL
import urllib

#Set library parameter
pd.set_option('display.float_format',  '{:.4f}'.format) #set pandas float format

#Read dataset format = csv
dataset = pd.read_csv("D:\Danang\database\commodity\commodity_trade_statistics_data.csv")
df_full_data = dataset

#Database parameter
DRIVER_NAME = 'ODBC Driver 17 for SQL Server'
SERVER_NAME = 'LAPTOP-QTF91CRJ'
DATABASE_NAME = 'TrainingWorks'

connection = urllib.parse.quote_plus(f'DRIVER={DRIVER_NAME};'
    f'SERVER={SERVER_NAME};'
    f'DATABASE={DATABASE_NAME};'
    'Trusted_Connection=yes;')

#analytical parameter
#############################################################################################
# function: commsrc_after
full_year = 2012 #must be int
commodity = 'all' #choose all to process all data

#function: ranks_flow
ranks_flow = 'Import' #choose top 10 flow 

#clean category should not included for the current commodity
clean_comm_code = '' #choose '' for all commodity

#############################################################################################
#Find data type
dfc_full = df_full_data.copy() #copy full data to dfc_full dataframe
#change comm code to str
dfc_full['comm_code'] = dfc_full['comm_code'].astype(str)

#create function to search commodity and Import-export flow, use all to take all the flow
def commsrc_after(comm,after):
    if comm != 'all':
        commsearch = dfc_full[dfc_full['commodity'].str.contains(comm,regex=True,na=False)]
    else: commsearch = dfc_full[dfc_full['comm_code'] != 'TOTAL']
    temp = commsearch[commsearch['year'] >= after]
    return (temp)

def ranks(comm_eda,flow):
    if flow != 'all':
        dfc_eda = comm_eda[comm_eda['flow'] == flow]
    else: dfc_eda == comm_eda

    dfc_eda = dfc_eda.groupby(['country_or_area','quantity_name','year','flow'],as_index=False)['weight_kg','trade_usd'].sum() #to aggregate weight and trade usd
    dfc_eda['average_usd_kg'] = dfc_eda['trade_usd'] / dfc_eda['weight_kg'] #to create column which show average price each year
    dfc_eda['trade_usd_total'] = dfc_eda.groupby(['country_or_area'])['trade_usd'].transform(np.sum) #sum trade_usd over partition by country_or_area
    dfc_eda['weight_kg_total'] = dfc_eda.groupby(['country_or_area'])['weight_kg'].transform(np.sum) #sum weight_kg over partition by country_or_area
    dfc_eda['average_price_years'] = dfc_eda.groupby(['country_or_area'])['average_usd_kg'].transform(np.median) #average average_price_year over partition by country_or_area
    dfc_eda['Rank_trade_usd'] = dfc_eda.trade_usd_total.rank(method='dense',ascending=False).astype(int) #create dense rank for total trade_usd
    dfc_eda['Rank_weight_kg'] = dfc_eda.weight_kg_total.rank(method='dense',ascending=False).astype(int) #create dense rank for total weight_kg
    dfc_eda['Rank_avg_usd_weight'] = dfc_eda.average_price_years.rank(method='dense',ascending=False).astype(int) #create dense rank for average_usd_weight
    return(dfc_eda)

#create function to check top 10 country with the highest price in latest year
def ranks_latest(top_eda,ranks_column,latest_flow):
    temp = top_eda[top_eda['year']==top_eda['year'].max()]
    temp_ranks = ranks(temp,latest_flow)
    df_ranks = temp_ranks[temp_ranks[ranks_column]<=10].sort_values(ranks_column)
    return(df_ranks)

###################### CLEANING PROCESS ################################

#take data for chosen commodity, in all flow and start with chosen year
comm_clean = commsrc_after(commodity,full_year)

#drop all index which contain certain comm_code which not relevant with the commodity analyzed
drop = comm_clean[comm_clean['comm_code'] == clean_comm_code].index
comm_clean.drop(drop,inplace=True)

#drop nan value
null_clean = comm_clean[comm_clean['weight_kg'].isna()]
a = null_clean.index
comm_clean.drop(a,inplace=True)
finish_clean = comm_clean.reset_index()

#find duplicate value and delete
dupli_clean = comm_clean[comm_clean.duplicated()]
if dupli_clean['flow'].count() <= 0:
    print('No duplicate')
else: comm_clean.drop(dupli_clean,inplace=True)

#Write/export to database @ mssql server
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(connection))
comm_clean.to_sql('commodity_data', schema='dbo', con=engine, if_exists='replace')