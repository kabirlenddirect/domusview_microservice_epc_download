import ast
import os
import logging
from dotenv import load_dotenv
load_dotenv()


class Config(object):
    """Base config, uses staging database server."""
    DEBUG = False
    TESTING = False  # Set this to True to not save any of the results to mongodb
    LOG_LEVEL = logging.INFO
    IDENTITY = 'Central Copy Process'

    API_IDENTITY = ''
    SECRET_KEY = os.getenv('SECRET_KEY')

   # GOOGLE_APPLICATION_CREDENTIALS = ast.literal_eval(os.getenv('GOOGLE_APPLICATION_CREDENTIALS').replace('\n', '\\n'))
    
    SUBSCRIBER_CONFIG = {
        'PROJECT_NAME': 'domusview',
        'SUBSCRIBER_NAME' : '',
        'NEW_TOPIC_NAME': 'markets-new-primary',
        'CHANGED_TOPIC_NAME': 'markets-updates-primary'
    }

    VERIFY_SSL = False

    MDB_USER = os.getenv('MDB_USER')
    MDB_PASS = os.getenv('MDB_PASS')
    MDB_ADDRESS = os.getenv('MDB_ADDRESS')

    SLB_SECRET_KEY = os.getenv('SLB_SECRET_KEY')
    SLB_IP = os.getenv('SLB_IP')

    updates_uri = f'mongodb+srv://{MDB_USER}:{MDB_PASS}@{MDB_ADDRESS}/?authSource=admin'
    live_uri = f'mongodb+srv://{MDB_USER}:{MDB_PASS}@{MDB_ADDRESS}/?authSource=admin'

    MONGO_DB_MAX_RETRY_NUMBER = 3

    MONGODB_BINDS = {
        'sources': 'sources',
    }
    # {collection : identifier_col_in_queue, k:v}
    COLUMNS = {
        "centralsecondary" : 'main_references',
        'ulistsecondary': 'listings_l_refs',
        'urentsecondary': 'rentals_l_refs'
    }

    SQL_USER = os.getenv('SQL_USER')
    SQL_PASS = os.getenv('SQL_PASS')
    SQL_IP = os.getenv('SQL_IP')
    SQL_PORT = os.getenv('SQL_PORT')

    URI = f"mysql://{SQL_USER}:{SQL_PASS}@{SQL_IP}:{SQL_PORT}/updates_log?charset=utf8"
    PRIMARY_DB_URI = f"mysql://{SQL_USER}:{SQL_PASS}@{SQL_IP}:{SQL_PORT}/lr_log?charset=utf8" #schema_first

    # LR Download Configs
    LR_DOWNLOAD_LINKS = {
        'whole': 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv',  #run this link to auto download csv
        'monthly': 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv',
        'yearly': 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2020.csv'
    }

    LR_VERBOSE = False
    LR_NEWDATA_BATCHSIZE = 20000
    LR_MAX_DOWNLOAD_CHUNKSIZE = 2 ** 20  # 2 ** 20 is around 5800 rows (1MB)
    LR_QUERY_MAX_RETRY = 3
    LR_DOWNLOAD_CONFIG = {'links': LR_DOWNLOAD_LINKS,
                            'max_chunksize': LR_MAX_DOWNLOAD_CHUNKSIZE,
                            'new_data_batchsize': LR_NEWDATA_BATCHSIZE,
                            'max_retry': LR_QUERY_MAX_RETRY,
                            'worker_thread_count': 2
                            }



class ProductionConfig(Config):
    """Uses production database server."""
    LOG_LEVEL = logging.INFO


class DevelopmentConfig(Config):
    DEBUG = True
    LOG_LEVEL = logging.DEBUG
    

class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    LOG_LEVEL = logging.DEBUG


Configuration = DevelopmentConfig


