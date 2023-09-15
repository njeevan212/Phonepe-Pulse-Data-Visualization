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