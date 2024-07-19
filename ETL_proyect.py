#Importing the required libreries
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import sqlite3

#Code to ETL operations

#massege funtion
def load_massege(massege):
    '''funtion to comunicate the start and end of the steps'''
    with open('code_log_massege.txt','a') as load_file:
        load_file.write(massege + '\n')

'''FIRST STEP: Extract'''
def Extract(url, table_attribs):
    '''funtion to Extract the table of the URL'''
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content,'html.parser') #Parsers are tools that break down HTML into a tree of elements that can be navigated and manipulated through code.
        table = soup.find('table')

        rows = []
        for tr in table.find_all('tr')[1:]: #skip the header row
            cells = []
            for td in tr.find_all('td'):
                cells.append(td.text.strip()) #.text to obtain all text contained an HTML element       .strip() to removes whitespace at the beginning and end of the string. This includes spaces, tabs, and line breaks.
            rows.append(cells)

        df = pd.DataFrame(rows,columns=table_attribs)
        load_massege('Successful extraction')
        return df
    
    else:
        return load_massege('unsuccessful extraction')


'''SECOND STEP: Transform'''
def Transform(df,columns_drop):
    '''funtion to delet some columns'''
    df_transformed = df.drop(columns_drop, axis=1)
    df_transformed['ingresos(usd)'] = df_transformed['ingresos(usd)'].str.replace(',', '').astype(float) # to remove ','
    df_transformed['ingresos(usd)'] = pd.to_numeric(df_transformed['ingresos(usd)']) # to convert the string to float

    df_transformed['ingresos(eur)'] = df_transformed['ingresos(usd)'] * 0.92 # 1 dollar = 0.92 euros today (19/07/2024)
    df_transformed['ingresos(cop)'] = df_transformed['ingresos(usd)'] * 4016.19# 1 dollar = 4016.19 pesos colombianos today (19/07/2024)

    load_massege('Succesful transformation')
    return df_transformed



'''THIRD STEP: Load'''
def load_CSV(df,output_path):
    '''funtion to load the Data Frame'''
    df.to_csv(output_path, index=False)
    load_massege('Succesful load')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection, if_exists='replace', index=False)
    load_massege('Data loaded to Database as a table')


'''Here, you define the required entities and call the relevant
functions in the correct order'''
url = 'https://es.wikipedia.org/wiki/Anexo:Empresas_más_grandes_de_América_Latina_por_ingresos'

load_massege('the start')

df = Extract(url,['nombre','razon','industria','ingresos(usd)','sede','fecha_reporte'])
print(df)

df = Transform(df,['razon','sede','fecha_reporte'])
print(df)

load_CSV(df,'Transformed_File.csv')
sql_connection = sqlite3.connect('database.db')
load_to_db(df, sql_connection, 'Empresas más grandes por ingresos')



