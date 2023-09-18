from git.repo.base import Repo
import pandas as pd
import mysql.connector 
from mysql.connector import Error
import streamlit as st
from streamlit_option_menu import option_menu
import streamlit.components.v1 as html
from  PIL import Image
import plotly.express as px

#streamlit run main.py 

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


st.title('Phonepe Pulse Data Visualization and Exploration')

with st.sidebar:
    choose = option_menu("PhonePe Pulse Menu", ["Aggregated Data", "Map Data", "Top Data", "India Geo"],
                         icons=['graph-up', 'graph-up', 'graph-up', 'graph-up'],
                         menu_icon="home", default_index=0,
                         styles={
                            "container": {"padding": "5!important", "background-color": "#fafafa"},
                            "icon": {"color": "#000", "font-size": "25px"}, 
                            "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
                            "nav-link-selected": {"background-color": "#1482e3"},
                        }
    )
    s_year = st.sidebar.slider("**Year**", min_value=2018, max_value=2023)
    s_quarter = st.sidebar.slider("**Quarter**", min_value=1, max_value=4)
    s_type = st.sidebar.selectbox('Type',["Transactions", "Users"], index=0)

if choose == "Aggregated Data":
    
    if s_type == "Transactions":
        
        cursor.execute(f"""SELECT CONCAT(UPPER(LEFT(State,1)) ,
                       LOWER(RIGHT(State,LENGTH(State)-1))) as State, 
                       SUM(Total_Transaction) as Total_Transaction, 
                       SUM(Total_Amount) as Total_Amount
                       FROM phonpe_pulse.agg_trans 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY State 
                       ORDER BY Total_Amount DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total_Transaction','Total_Amount'])
        
        st.write("### :green[Top 10 States Vs Total_Amount]")
        fig = px.bar(df,
                     x='State',
                     y='Total_Amount',
                     orientation='v',
                     hover_data=['Total_Transaction'],
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
        cursor.execute(f"""SELECT Transaction_Type, 
                       SUM(Total_Transaction) as Total_Transaction, 
                       SUM(Total_Amount) as Total_Amount
                       FROM phonpe_pulse.agg_trans 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY Transaction_Type 
                       ORDER BY Total_Amount DESC""")
        df = pd.DataFrame(cursor.fetchall(), columns=['Transaction_Type', 'Total_Transaction','Total_Amount'])
        
        st.write("### :green[Transaction_Type Vs Transaction Amount]")
        fig = px.pie(df, values='Total_Amount',
                             names='Transaction_Type',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Total_Transaction'],
                             labels={'Total_Transaction':'Total_Transaction'})

        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)
        
    if s_type == "Users":
        
        cursor.execute(f"""SELECT CONCAT(UPPER(LEFT(State,1)) ,
                       LOWER(RIGHT(State,LENGTH(State)-1))) as State, 
                       Mobile_Brand, 
                       SUM(Brand_User_Count) as Brand_User_Count
                       FROM phonpe_pulse.agg_user 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY State, Mobile_Brand 
                       ORDER BY Brand_User_Count DESC LIMIT 50""")
        df = pd.DataFrame(cursor.fetchall(), columns=['State','Mobile_Brand', 'Brand_User_Count'])
        
    
        st.write("### :green[State Vs Brand's Count]")
        fig = px.bar(df,
                     x='State',
                     y='Brand_User_Count',
                     orientation='v',
                     color='Mobile_Brand'
                    )
        st.plotly_chart(fig,use_container_width=True)
        
        cursor.execute(f"""SELECT Mobile_Brand, 
                       SUM(Brand_User_Count) as Brand_User_Count, 
                       SUM(Percentage) as Percentage
                       FROM phonpe_pulse.agg_user 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY Mobile_Brand 
                       ORDER BY Percentage DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['Mobile_Brand', 'Brand_User_Count','Percentage'])
        
        st.write("### :green[Top 10 Mobile_Brand Vs Brand_User_Count]")
        fig = px.bar(df,
                     x='Mobile_Brand',
                     y='Brand_User_Count',
                     orientation='v',
                     hover_data=['Percentage'],
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    
    
elif choose == "Map Data":
    
    
    if s_type == "Transactions":
        #print(s_year)
        #print(s_quarter)
        cursor.execute(f"""SELECT CONCAT(UPPER(LEFT(State,1)) ,
                       LOWER(RIGHT(State,LENGTH(State)-1))) as State, 
                       SUM(Count) as Transaction_Count, 
                       SUM(Amount) as Total_Amount
                       FROM phonpe_pulse.map_trans 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY State 
                       ORDER BY Total_Amount DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Transaction_Count','Total_Amount'])
        
        st.write("### :green[Top 10 States Vs Total_Amount]")
        fig = px.bar(df,
                     x='State',
                     y='Total_Amount',
                     orientation='v',
                     hover_data=['Transaction_Count'],
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
        
        cursor.execute(f"""SELECT CONCAT(UPPER(LEFT(District,1)) ,
                       LOWER(RIGHT(District,LENGTH(District)-1))) as District, 
                       SUM(Count) as Transaction_Count, 
                       SUM(Amount) as Total_Amount
                       FROM phonpe_pulse.map_trans 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY District 
                       ORDER BY Total_Amount DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Transaction_Count','Total_Amount'])
        
        st.write("### :green[Top 10 Districts Vs Total_Amount]")
        fig = px.bar(df,
                     x='District',
                     y='Total_Amount',
                     orientation='v',
                     hover_data=['Transaction_Count'],
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    if s_type == "Users":
        cursor.execute(f"""SELECT CONCAT(UPPER(LEFT(State,1)) ,
                       LOWER(RIGHT(State,LENGTH(State)-1))) as State, 
                       SUM(RegisteredUser) as Registered_User, 
                       SUM(AppUsed) as AppUsed
                       FROM phonpe_pulse.map_user 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY State 
                       ORDER BY Registered_User DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Registered_User','AppUsed'])
        
        st.write("### :green[Top 10 States Vs Registered User]")
        fig = px.bar(df,
                     x='State',
                     y='Registered_User',
                     orientation='v',
                     hover_data=['AppUsed'],
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
        
        cursor.execute(f"""SELECT CONCAT(UPPER(LEFT(District,1)) ,
                       LOWER(RIGHT(District,LENGTH(District)-1))) as District, 
                       SUM(RegisteredUser) as Registered_User, 
                       SUM(AppUsed) as AppUsed
                       FROM phonpe_pulse.map_user 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY District 
                       ORDER BY AppUsed DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['District', 'Registered_User','AppUsed'])
        
        st.write("### :green[Top 10 Districts Vs App  Used]")
        fig = px.bar(df,
                     x='District',
                     y='AppUsed',
                     orientation='v',
                     hover_data=['Registered_User'],
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
    
elif choose == "Top Data":
    
    
    if s_type == "Transactions":
        #print(s_year)
        #print(s_quarter)
        cursor.execute(f"""SELECT CONCAT(UPPER(LEFT(State,1)) ,
                       LOWER(RIGHT(State,LENGTH(State)-1))) as State, 
                       SUM(Transaction_Count) as Total_Transactions_Count, 
                       SUM(Transaction_Amount) as Total
                       FROM phonpe_pulse.top_trans 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY State 
                       ORDER BY Total DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Transactions_Count','Total_Amount'])
        
        st.write("### :green[Top 10 States Vs Transaction Amount]")
        fig = px.bar(df,
                     x='State',
                     y='Total_Amount',
                     orientation='v',
                     hover_data=['Transactions_Count'],
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
        cursor.execute(f"""SELECT Pincode, 
                       SUM(Transaction_Count) as Total_Transactions_Count, 
                       SUM(Transaction_Amount) as Total
                       FROM phonpe_pulse.top_trans 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY Pincode 
                       ORDER BY Total DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['Pincode', 'Transactions_Count','Total_Amount'])
        
        st.write("### :green[Top 10 Pincode Vs Transaction Amount]")
        fig = px.pie(df, values='Total_Amount',
                             names='Pincode',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             hover_data=['Transactions_Count'],
                             labels={'Transactions_Count':'Transactions_Count'})

        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)
        
    if s_type == "Users":
        cursor.execute(f"""SELECT CONCAT(UPPER(LEFT(State,1)) ,
                       LOWER(RIGHT(State,LENGTH(State)-1))) as State, 
                       SUM(RegisteredUsers) as Users
                       FROM phonpe_pulse.top_user 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY State 
                       ORDER BY Users DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Registered_Users'])
        
        st.write("### :green[Top 10 States Vs Registered Users]")
        fig = px.bar(df,
                     x='State',
                     y='Registered_Users',
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
        cursor.execute(f"""SELECT Pincode, 
                       SUM(RegisteredUsers) as Users
                       FROM phonpe_pulse.top_user 
                       WHERE Year = {s_year} and Quarter = {s_quarter} GROUP BY Pincode 
                       ORDER BY Users DESC LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(), columns=['Pincode', 'Registered_Users'])
        
        st.write("### :green[Top 10 Pincode Vs Registered Users]")
        fig = px.pie(df, values='Registered_Users',
                             names='Pincode',
                             color_discrete_sequence=px.colors.sequential.Agsunset,
                             labels={'Registered_Users':'Registered_Users'})

        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)
        
elif choose == "India Geo":
    
    if s_type == "Transactions":
        st.markdown("## :green[All State Vs Transactions Amount]")
        cursor.execute(f"""SELECT State, 
                       SUM(Count) as Total_Transactions, 
                       SUM(amount) as Total_amount 
                       FROM phonpe_pulse.map_trans 
                       WHERE Year = {s_year} and Quarter = {s_quarter} 
                       GROUP BY State 
                       ORDER BY State""")
        df1 = pd.DataFrame(cursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
        df_states = pd.read_csv('csv/states.csv')
        df1.State = df_states

        fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    color='Total_amount',
                    color_continuous_scale='Reds')
        
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig,use_container_width=True)
        
    if s_type == "Users":
        st.markdown("## :green[All State - User App Opened]")
        cursor.execute(f"""SELECT State, 
                       SUM(RegisteredUser) as Total_Users, 
                       SUM(AppUsed) as Total_Appopens 
                       FROM phonpe_pulse.map_user where Year = {s_year} and Quarter = {s_quarter} 
                       GROUP BY State ORDER BY State""")
        df1 = pd.DataFrame(cursor.fetchall(), columns=['State', 'Total_Users','Total_Appopens'])
        df_states = pd.read_csv('csv/states.csv')
        df1.Total_Appopens = df1.Total_Appopens.astype(float)
        df1.State = df_states
        
        fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                  featureidkey='properties.ST_NM',
                  locations='State',
                  color='Total_Appopens',
                  color_continuous_scale='balance')

        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig,use_container_width=True)
