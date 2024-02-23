# this script performs the ETL operations to Extract data about top 10 banks in the world from web , 
#                                            Transfom it to pre-defined settings and
#                                            load data into boths CSV files and database

# import libraries
import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

# set variables
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_file = r"C:\Users\Mahmoud\Documents\python\Top Banks\exchange_rate.csv"
df_columns = ["Name" ,"MC_USD_Billion" ]
csv_file = "Top Banks\.Largest_banks_data.csv"
db_name ="Top Banks\.Banks.db"
table_name = "Largest_banks"
log_file = "Top Banks\.code_log.txt"
conn = sqlite3.connect(db_name)



# Extract function
def extract(url):  
    """
    this function take URL of website that has data needed to extract
    and return pandas DataFrame object after perform webscraping
    """
    page = requests.get(url).text
    soup = BeautifulSoup(page,"html5lib")
    table = soup.find_all('tbody')[0]
    rows = table.find_all('tr')
    df = pd.DataFrame(columns = df_columns)
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            name = cols[1].find_all('a')[1].string
            mc = cols[2].string
            dict_records = {"Name": name,"MC_USD_Billion":float(mc)}
            df1 = pd.DataFrame(dict_records,index = [0])
            df = pd.concat([df,df1],ignore_index=True)
        else :
            continue
    return df
# transform function
def transform(df,exchange_r_f):
    """
    This function take DataFrame object which contains of two columns Name ,MC_USD_Billion and exchange rate file
    and return it with five columns after add MC_EUR_Billion , MC_GBP_Billion , MC_INR_Billion columns

    """
    exchange_rate = pd.read_csv(exchange_r_f)
    EUR = exchange_rate.loc[0,"Rate"]
    GBP = exchange_rate.loc[1,"Rate"]
    INR = exchange_rate.loc[2,"Rate"]

    MC_USD_list = df['MC_USD_Billion'].tolist()
    MC_EUR_list = [round(eur*EUR,2) for eur in MC_USD_list]
    MC_GBP_list = [round(gbp*GBP,2) for gbp in MC_USD_list]
    MC_INR_list = [round(inr*INR,2) for inr in MC_USD_list]

    df["MC_EUR_Billion"] = MC_EUR_list
    df["MC_GBP_Billion"] = MC_GBP_list
    df["MC_INR_Billion"] = MC_INR_list
    return df
# Load functions
def load_in_csv(df,file_path):
    """
    Load DataFrame into CSV
    """
    df.to_csv(file_path)
def load_in_db(df,sql_connection,tn):
    """Load into database"""
    df.to_sql(tn,sql_connection,if_exists='replace',index = True)
# Query function
def query(quary_statment,sql_connection):
    """This function allow to apply queries on table in database to return a spasific data"""
    print(quary_statment)
    output = pd.read_sql(quary_statment,sql_connection)
    print(output)
# Log function
def log(file,massage):
    ''' This function logs the mentioned message of a given stage of the
      code execution to a log file. Function returns nothing'''
    timeformate =r"%Y-%m-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timeformate)
    with open(file,'a') as f:
        f.write(timestamp +" : "+ massage + "\n")

# call functions 
log(log_file,"Extract Data")
my_df = extract(url)
log(log_file,"Transform Data")
transform(my_df,exchange_rate_file)
log(log_file,"Load Data in csv file")
load_in_csv(my_df,csv_file)
log(log_file,"load Data in database")
load_in_db(my_df,conn,table_name)

stat1 = f"SELECT * FROM {table_name}"
stat2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
stat3 = f"SELECT Name from {table_name} LIMIT 5"
log(log_file,"Print the contents of the entire table")
query(stat1,conn)
log(log_file,"Print the average market capitalization of all the banks in Billion USD")
query(stat2,conn)
log(log_file,"Print only the names of the top 5 banks")
query(stat3,conn)
