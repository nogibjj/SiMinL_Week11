[![CI](https://github.com/nogibjj/SiMinL_Week5/actions/workflows/hello.yml/badge.svg)](https://github.com/nogibjj/SiMinL_Week5/actions/workflows/hello.yml)

# SiMinL_MiniProj5

# Requirements
Connect to a SQL database
Perform CRUD operations (Create, Read, Update, Delete)
Write at least two different SQL queries
Grading Criteria 

# Purpose 
The purpose of this project is to create CRUD actions using SQLite and Python, and to build an ETL-query pipeline. Here, xxx is used as a sample dataset. It is converted to a .db file, and CRUD actions are conducted.

# Preparation
1. Open codespaces
2. Wait for container to be built and virtual environment to be activated with requirements.txt installed
3.Extract: run make extract
4. Transform and load: run make transform_load
5. Query: run make query or alternatively write your own query using python main.py general_query <insert query>

# Sample CRUD Operations
Create: python main.py create_record 'Computer Science' 'STEM' 1500 1200
Read: python main.py read_data()
Update: python main.py update_record 1 'Electrical Engineering' 'STEM' 2000 1500
Delete: python main.py delete_record 1
