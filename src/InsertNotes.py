import os
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import uuid
import bson
from bson.binary import Binary, UuidRepresentation

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

def upsert_db_one(data, col_name, dbs_name='AIDF_NLP_Capstone'):
    DB = connect_db(dbs_name)
    collection = DB[col_name]
    try:
        filter_criteria = {"_id": data["_id"]}
        collection.update_one(filter_criteria, {"$set": data}, upsert=True)
    except Exception as e:
        print(e)
        pass


def create_id(cik, date, type):
    _id = uuid.uuid3(uuid.NAMESPACE_DNS, str(cik) + date + type)
    _id = bson.Binary.from_uuid(_id, uuid_representation=UuidRepresentation.PYTHON_LEGACY)
    return _id

def insert_notes(data_dir):
    all_files = [os.path.join(root, file)
                 for root, dirs, files in os.walk(data_dir)
                 for file in files if file.endswith(".txt")]
    for i, file in enumerate(all_files):
        filename = os.path.basename(file)
        cik = filename.split('_')[0]
        filing_date = filename.split('_')[1]
        filing_type = '10-K'
        _id = create_id(cik, filing_date, filing_type)
        print(f"{i + 1}", filename)
        with open(file, 'r', encoding='utf-8') as f:
            notes = f.read()
        print(f'{i + 1}. ', file)
        record = {
            '_id': _id,
            'cik': int(cik),
            'filing_date': filing_date,
            'type': filing_type,
            'notes': notes
        }
        upsert_db_one(record, 'Level2_10k_Notes')

def insert_tables(data_dir):
    all_dirs = [os.path.join(root, dir)
                for root, dirs, files in os.walk(data_dir)
                for dir in dirs if dir.endswith('html')]
    for i, file in enumerate(all_dirs):
        filename = os.path.basename(file)
        print(filename)
        cik = filename.split('_')[0]
        filing_date = filename.split('_')[1]
        filing_type = '10-K'
        _id = create_id(cik, filing_date, filing_type)
        table_files = [os.path.join(file, f) for f in os.listdir(file)]
        tables = []
        for table_file in table_files:
            with open(table_file, 'r', encoding='utf-8') as f:
                tables.append(f.read().strip())
        print(f'{i + 1}. ', file)
        record = {
            '_id': _id,
            'cik': int(cik),
            'filing_date': filing_date,
            'type': filing_type,
            'tables': tables
        }
        upsert_db_one(record, 'Level2_10k_Tables')

_ = load_dotenv(find_dotenv())

DATA_DIR = r'C:\Users\e0638886\Desktop\ExtractNotes'
NOTES_DIR = os.path.join(DATA_DIR, 'texts')
TABLES_DIR = os.path.join(DATA_DIR, 'tables')

# insert_notes(NOTES_DIR)
insert_tables(TABLES_DIR)

# all_files = [os.path.join(root, dir)
#                 for root, dirs, files in os.walk(TABLES_DIR)
#                 for dir in dirs if dir.endswith('html')]

# print(all_files)
# print(len(all_files))

# client = MongoClient('mongodb://aidf_alternativeData:AidfNlp001.@10.230.252.3:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')
# print(client.list_database_names())
# db = client['AIDF_NLP_Capstone']
# collection = db['Level2_10k_Notes']