'''Description: This code will connect and extract past 10 days of APOD data from NASA API.
                Also, the data will be stored as JSON file with file name as date and using regex the values will be extracted from the response to store in MySQL Database.
                All these data can be visualized through Streamlit library based on the date and data source (MySQL/JSON)'''


import requests
import datetime
import re
import mysql.connector
import json
import streamlit as st
import pandas as pd


#Fucntion to extract the values form the response using the regex pattern

def extract_with_pattern(pattern, text):
    match = re.findall(pattern, text)
    return match


#Fucntion to create and connect with the database apod_database

def connect_to_mysql_database(config):
    try:
        conn = mysql.connector.connect(**config)
        cursor= conn.cursor()
        if conn.is_connected():
            cursor.execute("CREATE DATABASE IF NOT EXISTS apod_database")
            cursor.execute("USE apod_database")
            # print("Connected to MySQL database")
            return conn
    except mysql.connector.Error as e:
        print("MySQL error:", e)
        return None
    cursor.close()


#Fucntion to create table 'apod_data' in the database apod_database

def create_table(conn):
    if conn is not None:
        cursor = conn.cursor()
        create_table_sql = '''
        CREATE TABLE IF NOT EXISTS apod_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE,
            explanation TEXT,
            media_type VARCHAR(100),
            title VARCHAR(255),
            image_url VARCHAR(255)
        );
        '''
        cursor.execute(create_table_sql)
        cursor.execute("TRUNCATE TABLE apod_data")
        # print("deleted all rows (if exists)")
        conn.commit()
        cursor.close()
        # print("Table 'apod_data' created (if not exists).")


#Function to insert data into the table 'apod_data'

def insert_data(conn, data):
    if conn is not None:
        cursor = conn.cursor()
        insert_sql = "INSERT INTO apod_data (date, explanation, media_type, title, image_url) VALUES (%s, %s, %s, %s, %s);"
        try:
            cursor.execute(insert_sql, data)
            conn.commit()
            # print("Inserted successfully.")
        except mysql.connector.Error as e:
            conn.rollback()
            print("MySQL error:", e)
        cursor.close()


# Function to extract date form the table 'apod_data'

def extract_value_from_table(conn):
        if conn is not None:
            # print("Hi")
            cursor = conn.cursor()
            cursor.execute("SELECT date from apod_data")
            rows = cursor.fetchall()
            cursor.close()
            return rows
            

#API request for NASA APOD data for past days

api_key = 'E7BycvBXJyvX2sHlaTd4pzrNr2k7I6rpQUEtyIDz'
today = datetime.date.today()
start_date = today - datetime.timedelta(days=9)
url = f'https://api.nasa.gov/planetary/apod?api_key={api_key}&start_date={start_date}'
response = requests.get(url)
response_text = response.text


# Using regex extracting data from the response.text

date_pattern = r'"date":"([^"]+)"'
explanation_pattern = r'"explanation":"([^"]+)"'
mediatype_pattern = r'"media_type":"([^"]+)"'
title_pattern = r'"title":"([^"]+)"'
url_pattern = r'"url":"([^"]+)"'

current_date = extract_with_pattern(date_pattern, response_text)
explanation = extract_with_pattern(explanation_pattern, response_text)
media_type = extract_with_pattern(mediatype_pattern, response_text)
title = extract_with_pattern(title_pattern, response_text)
url = extract_with_pattern(url_pattern, response_text)


# Database connection

database_config = {
                        "host": "localhost",
                        "username" : "root",
                        "password":"mysql@12345;" 
                    }
conn = connect_to_mysql_database(database_config)
if conn is not None:
    create_table(conn)
    for i in range(len(current_date)):
        data = [current_date[i], explanation[i], media_type[i], title[i], url[i]]
        insert_data(conn, data)


#JSON file creation part

if response.status_code == 200:
    data = response.json()
    for i in range(len(data)):
        filename = data[i]["date"]
        with open(filename, 'w') as json_file:
            json.dump(data[i], json_file, indent=4)
        # print(f"Data saved as {filename}")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")


# Streamit part

dates = extract_value_from_table(conn)
choices = [date[0].strftime('%Y-%m-%d') for date in dates]
st.title(':red[NASA DATA]')
st.info("This page will give you the NASA APOD Data for past 10 days")
date = st.selectbox("Select a date:", choices)
data_source = st.radio("Select Data source: ", ('MySQL', 'JSON'))
button = st.button("Retrieve Data")

#Data will fetch from MySQL and JSON based on the input

if (button):
    if (data_source == "MySQL"):
        cursor = conn.cursor()
        query =f"SELECT date, title, explanation, image_url from apod_data where date = %s"
        cursor.execute(query, (date,))
        data = cursor.fetchall()
        my_data = [("Date",data[0][0].strftime('%Y-%m-%d')), ("Title",data[0][1]), ("Explanation", data[0][2]), ("Image_url", data[0][3])]
        df = pd.DataFrame(my_data)
        st.dataframe(my_data)
        cursor.close()
        conn.close()
    elif(data_source == "JSON"):
        file_name = date
        try:
            with open(file_name, 'r') as file:
                data = json.load(file)
                my_data = [("Date",data["date"]), ("Title",data["title"]), ("Explanation", data["explanation"]), ("Image_url", data["url"])]
                df = pd.DataFrame(my_data)
                st.dataframe(my_data)
        except FileNotFoundError as e:
                st.write(e)
