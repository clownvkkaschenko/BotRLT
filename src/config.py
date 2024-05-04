import os

from dotenv import load_dotenv

load_dotenv()


uri = os.environ.get('URI')
db_name = os.environ.get('DB_NAME')
collection_name = os.environ.get('COLLECTION_NAME')
tg_token = os.environ.get('TG_TOKEN')
