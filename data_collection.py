#import git
from git.repo.base import Repo
import os
import json
import pandas as pd
import mysql.connector 
from mysql.connector import Error

#Clone data from Phonepe pulse repo
#Repo.clone_from("https://github.com/PhonePe/pulse.git", "/home/jeeva/Projects/python/PhonePe_Pulse/phonepe_data/")


#Connection to MYSQL
connection = mysql.connector.connect(
        host ="localhost",
        user = "root",
        password = "@Jeeva.Arul212",
        #database= "phonpe_pulse"
)
cursor = connection.cursor()

#Create DB
cursor.execute("CREATE DATABASE IF NOT EXISTS phonpe_pulse")

#Create agg_transaction_table
query_agg_trans = """CREATE TABLE IF NOT EXISTS phonpe_pulse.agg_trans (
                        State varchar(100), 
                        Year int, 
                        Quarter int, 
                        Transaction_Type varchar(100), 
                        Total_Transaction BIGINT, 
                        Total_Amount double)"""
cursor.execute(query_agg_trans)

#Create agg_user table
query_agg_user = """CREATE TABLE IF NOT EXISTS phonpe_pulse.agg_user (
                        State varchar(100), 
                        Year int, 
                        Quarter int, 
                        Mobile_Brand varchar(100), 
                        Brand_User_Count BIGINT, 
                        Percentage double)"""
cursor.execute(query_agg_user)


#Create map_transaction table
query_map_transaction = """CREATE TABLE IF NOT EXISTS phonpe_pulse.map_trans(
                                State varchar(100), 
                                Year int, 
                                Quarter int, 
                                District varchar(100), 
                                Count BIGINT, 
                                Amount double)"""
cursor.execute(query_map_transaction)



#Create map_user table
query_map_user = """CREATE TABLE IF NOT EXISTS phonpe_pulse.map_user (
                        State varchar(100), 
                        Year int, 
                        Quarter int, 
                        District varchar(100), 
                        RegisteredUser BIGINT, 
                        AppUsed BIGINT)"""
cursor.execute(query_map_user)



#Create top_transaction table
query_top_transaction = """CREATE TABLE IF NOT EXISTS phonpe_pulse.top_trans (
                                State varchar(100), 
                                Year int, 
                                Quarter int, 
                                Pincode int, 
                                Transaction_Count BIGINT, 
                                Transaction_Amount double)"""
                        
cursor.execute(query_top_transaction)



#Create top_user table
query_top_user = """CREATE TABLE IF NOT EXISTS phonpe_pulse.top_user (
                        State varchar(100), 
                        Year int, 
                        Quarter int, 
                        Pincode int, 
                        RegisteredUsers int)"""
cursor.execute(query_top_user)

#Common function
def changeJsonToCsv(path, columns, columnHeaders, dataField, type, file_name):
    common_list = os.listdir(path)
    #print(path)
    for state in common_list:
        cur_state = path + state + "/"
        agg_year_list = os.listdir(cur_state)
    
        for year in agg_year_list:
            cur_year = cur_state + year + "/"
            agg_file_list = os.listdir(cur_year)
            
            for file in agg_file_list:
                cur_file = cur_year + file
                data = open(cur_file, 'r')
                json_file = json.load(data)

                if json_file['data'][dataField] is not None and type != 4:
                    for i in json_file['data'][dataField]:
                        if type == 1:
                            x = i['name']
                            y = i['paymentInstruments'][0]['count']
                            z = i['paymentInstruments'][0]['amount']
                        elif type == 2:
                            x = i["brand"]
                            y = i["count"]
                            z = i["percentage"]
                        elif type == 3:
                            x = i["name"]
                            y = i["metric"][0]["count"]
                            z = i["metric"][0]["amount"]
                        elif type == 5:
                            x = i['entityName']
                            y = i['metric']['count']
                            z = i['metric']['amount']
                        elif type == 6:
                            x = i['name']
                            y = i['registeredUsers']
                        
                        columns[columnHeaders['3']].append(x)
                        columns[columnHeaders['4']].append(y)
                        if type != 6:
                            columns[columnHeaders['5']].append(z)
                        columns[columnHeaders['0']].append(state)
                        columns[columnHeaders['1']].append(year)
                        columns[columnHeaders['2']].append(int(file.strip('.json')))
                        
                if json_file['data'][dataField] is not None and type == 4:
                    for i in json_file['data'][dataField].items():
                        x = i[0]
                        y = i[1]["registeredUsers"]
                        z = i[1]['appOpens']
                        
                        columns[columnHeaders['3']].append(x)
                        columns[columnHeaders['4']].append(y)
                        columns[columnHeaders['5']].append(z)
                        columns[columnHeaders['0']].append(state)
                        columns[columnHeaders['1']].append(year)
                        columns[columnHeaders['2']].append(int(file.strip('.json')))
                        
    df_common = pd.DataFrame(columns)
    #print(df_common)
    df_common.to_csv('/home/jeeva/Projects/python/PhonePe_Pulse/csv/'+ file_name ,index=False)
    if type == 1:
        for i,row in df_common.iterrows():
            query = """INSERT INTO phonpe_pulse.agg_trans VALUES (%s,%s,%s,%s,%s,%s)"""
            cursor.execute(query, tuple(row))
            connection.commit()
        print("Data saved")
    elif type == 2:
        for i,row in df_common.iterrows():
            query = "INSERT INTO phonpe_pulse.agg_user VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(query, tuple(row))
            connection.commit()
        print("Data saved")
    elif type == 3:
        for i,row in df_common.iterrows():
            query = "INSERT INTO phonpe_pulse.map_trans VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(query, tuple(row))
            connection.commit()
        print("Data saved")
    elif type == 4:
        for i,row in df_common.iterrows():
            query = "INSERT INTO phonpe_pulse.map_user VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(query, tuple(row))
            connection.commit()
        print("Data saved")
    elif type == 5:
        for i,row in df_common.iterrows():
            query = "INSERT INTO phonpe_pulse.top_trans VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(query, tuple(row))
            connection.commit()
        print("Data saved")
    elif type == 6:
        for i,row in df_common.iterrows():
            query = "INSERT INTO phonpe_pulse.top_user VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(query, tuple(row))
            connection.commit()
        print("Data saved")
        
        
aggregated_trans_path = "/home/jeeva/Projects/python/PhonePe_Pulse/phonepe_data/data/aggregated/transaction/country/india/state/"   
agg_trans_columns = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_Type': [], 
                     'Total_Transaction': [], 'Total_Amount': []}
agg_trans_column_header = {'0' :'State', '1' : 'Year' , '2' : 'Quarter' ,'3' : 'Transaction_Type', 
                           '4' : 'Total_Transaction','5' : 'Total_Amount'}
#changeJsonToCsv(aggregated_trans_path, agg_trans_columns, agg_trans_column_header, 'transactionData', 1, "1.csv")    




aggregated_user_path = "/home/jeeva/Projects/python/PhonePe_Pulse/phonepe_data/data/aggregated/user/country/india/state/"
agg_user_columns = {'State': [], 'Year': [], 'Quarter': [], 
                    'Mobile_Brand': [], 'Brand_User_Count': [], 'Percentage': []}
agg_user_column_header = {'0' :'State', '1' : 'Year' , '2' : 'Quarter' ,'3' : 'Mobile_Brand', 
                           '4' : 'Brand_User_Count','5' : 'Percentage'}
#changeJsonToCsv(aggregated_user_path, agg_user_columns, agg_user_column_header, 'usersByDevice', 2, "2.csv") 




map_trans_path = "/home/jeeva/Projects/python/PhonePe_Pulse/phonepe_data/data/map/transaction/hover/country/india/state/"
map_trans_columns = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [], 'Amount': []}
map_trans_column_header = {'0' :'State', '1' : 'Year' , '2' : 'Quarter' ,'3' : 'District', 
                           '4' : 'Count','5' : 'Amount'}
#changeJsonToCsv(map_trans_path, map_trans_columns, map_trans_column_header, 'hoverDataList', 3, "3.csv") 




map_user_path = "/home/jeeva/Projects/python/PhonePe_Pulse/phonepe_data/data/map/user/hover/country/india/state/"
map_user_columns = {"State": [], "Year": [], "Quarter": [], "District": [], "RegisteredUser": [], "AppUsed": []}
map_user_column_header = {'0' :'State', '1' : 'Year' , '2' : 'Quarter' ,'3' : 'District', 
                           '4' : 'RegisteredUser','5' : 'AppUsed'}
#changeJsonToCsv(map_user_path, map_user_columns, map_user_column_header, 'hoverData', 4, "4.csv")



top_trans_path = "/home/jeeva/Projects/python/PhonePe_Pulse/phonepe_data/data/top/transaction/country/india/state/"
top_trans_columns = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_Count': [], 'Transaction_Amount': []}
top_trans_column_header = {'0' :'State', '1' : 'Year' , '2' : 'Quarter' ,'3' : 'Pincode', 
                           '4' : 'Transaction_Count','5' : 'Transaction_Amount'}
#changeJsonToCsv(top_trans_path, top_trans_columns, top_trans_column_header, 'pincodes', 5, "5.csv")



top_user_path = "/home/jeeva/Projects/python/PhonePe_Pulse/phonepe_data/data/top/user/country/india/state/"
top_user_columns = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'RegisteredUsers': []}
top_user_column_header = {'0' :'State', '1' : 'Year' , '2' : 'Quarter' ,'3' : 'Pincode', 
                           '4' : 'RegisteredUsers'}
#changeJsonToCsv(top_user_path, top_user_columns, top_user_column_header, 'pincodes', 6, "6.csv")


