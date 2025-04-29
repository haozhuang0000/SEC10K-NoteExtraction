# from seleniumbase import Driver
import argparse
import time
import urllib
# from selenium import webdriver
from datetime import datetime
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import uuid
import bson
from bson.binary import Binary, UuidRepresentation
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

_ = load_dotenv(find_dotenv())
def get_driver(isHeadless=False):
    # driver = Driver(uc=True, binary_location='/opt/airflow/dags/data/chromedriver.exe', headless2=isHeadless)
    options = Options()
    options.set_capability('se:recordVideo', True)
    options.set_capability('se:screenResolution', '1920x1080')
    options.set_capability('se:name', 'test_visit_basic_auth_secured_page (ChromeTests)')
    # options.add_argument("--proxy-server=http://airflow-mitmproxy-1:8080")
    # options.add_argument("--ignore-certificate-errors")
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')
    print("adding options")
    # remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote('http://selenium-hub:4444', options=options)
    return driver

def connect_db(dbs='AIDF_NLP_Capstone'):
    DB_URL = os.environ['LOCAL_URL']
    client = MongoClient(DB_URL)
    db = client[dbs]
    return db

def insert_db(data, col_name, dbs_name='AIDF_NLP_Capstone'):
    if isinstance(data, pd.DataFrame):
        DB = connect_db(dbs=dbs_name)
        collection = DB[col_name]
        collection.insert_many(data.to_dict('records'))
    if isinstance(data, list):
        DB = connect_db(dbs=dbs_name)
        collection = DB[col_name]
        collection.insert_many(data)

def insert_db_one(data, col_name, dbs_name='AIDF_NLP_Capstone'):
    DB = connect_db(dbs_name)
    collection = DB[col_name]
    try:
        if isinstance(data, dict):
            collection.insert_one(data)
        elif isinstance(data, pd.DataFrame):
            collection.insert_one(data.to_dict('records')[0])
    except Exception as e:
        print(e)
        pass

def create_id(cik, date, type):
    _id = uuid.uuid3(uuid.NAMESPACE_DNS, str(cik) + date + type)
    _id = bson.Binary.from_uuid(_id, uuid_representation=UuidRepresentation.PYTHON_LEGACY)
    return _id

def determine_quarter(date_str):
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month = date.month
    if (month == 1) or (month == 2) or (month == 3):
        return 1
    elif (month == 4) or (month == 5) or (month == 6):
        return 2
    elif (month == 7) or (month == 8) or (month == 9):
        return 3
    else:
        return 4  # Covers October, November, December


def check_data_nonexist(key, value, collection, dbs_name='AIDF_NLP_Capstone'):

    find_cursor = collection.find({key: value})
    if [i for i in find_cursor] == []:
        return True
    elif [i for i in find_cursor] != []:
        return False


# if __name__ == '__main__':
#
#     driver = get_driver(True)
#     driver.get('https://n.news.naver.com/mnews/article/005/0001606450')
#     print(driver.page_source)