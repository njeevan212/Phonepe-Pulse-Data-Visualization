# Phonepe Pulse Data Visualization and Exploration
   The Phonepe pulse Github repository contains a large amount of data related to
various metrics and statistics. The goal is to extract this data and process it to obtain
insights and information that can be visualized in a user-friendly manner.

# Skill Set
Python, Streamlit, Pandas, Plotly and MYSQL

# Approches

1. Extract data from the Phonepe pulse Github repository through scripting and
clone it(https://github.com/PhonePe/pulse)
2. Transform the data into a suitable format and perform any necessary cleaning
and pre-processing steps.
3. Insert the transformed data into a MySQL database for efficient storage and
retrieval.
4. Create a live geo visualization dashboard using Streamlit and Plotly in Python
to display the data in an interactive and visually appealing manner.
5. Fetch the data from the MySQL database to display in the dashboard.
6. Provide at least 10 different dropdown options for users to select different
facts and figures to display on the dashboard.

# Extract data from PhonePe Github
   Using Repo kibrary we can get the json files phonepe pulse github and saved it in our local machine(Clone).
   
Repo.clone_from("https://github.com/PhonePe/pulse.git", "/home/jeeva/Projects/python/PhonePe_Pulse/phonepe_data/")

# Data Transformation
   There is a common method (changeJsonToCsv)written in data_collection.py file, with that method, we can convert the phonepe json data in DataFrame using Pandas and Json(Inbuilt) library.

# Data Insertion & Visualize
   Once converted the JSON file to Dataframe, we can save it in Mysql database and using SELECT query fetch the appropriate fileds from each data set and display the data using plotly and stremalit library.
