from time import sleep
import sqlalchemy as db
from sqlalchemy import exc
from config import Configuration as Config
import pandas as pd
from time import sleep
import datetime
from data_processor import DataPreparer
from dateutil import parser
from date_checker import DateChecker
from pydantic import BaseModel, validator, ValidationError
from typing import Optional
import re


class transaction_id_download_id(BaseModel):
    '''Dataclass to validate pubsub message'''
    transaction_id: str
    download_id: str

    @validator("transaction_id")
    def validate_transaction_id(transaction_id):
        acceptance = re.search("^.{8}-.{4}-.{4}-.{4}-.{12}$", transaction_id)
        if not acceptance:
            raise ValueError("transaction_id is not formatted correctly")            
        return transaction_id

    @validator("download_id")
    def validate_download_id(download_id):
        if  len(download_id) != 8:
            raise ValueError("download_id should have 8 characters")
        return download_id



class MaxRetryError(Exception):
    '''Raise error if set max number of queries is reached.'''
    pass

class RawSQLHandler:
    '''work with SQL data in Python'''
    def __init__(self):
        '''connect to the schema and setup datapreparer object'''
        self.engine = db.create_engine(Config.PRIMARY_DB_URI, pool_pre_ping=True)
        self.data_preparer = DataPreparer()
        self.date_checker = DateChecker()


    def display(self):
        '''output the SQL database in the IDE as a pandas dataframe'''
        conn = self.engine.connect()
        #date_downloaded = pd.read_sql('SELECT * FROM date_downloaded', conn)
        data = pd.read_sql('SELECT * FROM lr_log', conn)
        print(data.head)
        conn.close()


    def retrieve_download_date(self):
        '''
        get the date from the SQL (date_downloaded) database
        code from https://towardsdatascience.com/sqlalchemy-python-tutorial-79a577141a91
        '''
        conn = self.engine.connect() 
        metadata = db.MetaData()
        date_downloaded_table = db.Table('date_downloaded', metadata, autoload=True, autoload_with=self.engine)
        query = db.select([date_downloaded_table]) 
        ResultProxy = conn.execute(query)
        Result = ResultProxy.fetchall()
        date_downloaded = Result[0][0]
        conn.close()
        return date_downloaded


    def get_current_date(self):
        '''
        Get now's time as an offset aware date (timezones considered)
        This must be done to then compare now's date with the offset aware date that is extracted from the website
        '''
        current_date = datetime.datetime.now()
        return current_date


    def update_download_date(self):
        '''write the current date to the SQL database (date_downloaded)
           This function should only be called when the main download is happening '''
        con = self.engine.connect()
        current_date = self.get_current_date()
        df = pd.DataFrame(data=[current_date], columns=['date_downloaded'])
        df.to_sql(name='date_downloaded', schema='lr_log',con=con, if_exists='replace', chunksize=500, index=False)
        con.close()


    def convert_date_to_offset_aware(self, date):
        '''
        adds timezone consideration to the date so it is comparable with other dates in this format
        pass datetime object as an argument
        '''
        date = date.\
        replace(tzinfo=datetime.timezone.utc)
        date_and_timezone = date.isoformat()
        formatted_date = parser.parse(date_and_timezone)
        return formatted_date
        

    def check_for_website_update(self):
        ''' 
        Check whether there has been an update on the website
        '''
        date_modified = self.date_checker.find_date_modified() 
        date_last_checked = self.retrieve_download_date()
        date_last_checked = self.convert_date_to_offset_aware(date_last_checked)
        return (date_last_checked <  date_modified)


    def pandas_to_sql(self, df, con):
            """
            format data from csv to be ready for SQL database
            """
            retry_num = 1
            while True:
                try:
                    df.to_sql(name='lr_log', schema='lr_log',con=con, if_exists='append', chunksize=500, index=False)
                    break
                except (exc.OperationalError, exc.InternalError):
                    try:
                        con.close()
                    except NameError:
                        pass
                    if retry_num == 3:
                        raise MaxRetryError(
                            f'Number of queries exceeded maximum (max = {3})')
                    retry_num += 1
                    con = self.engine.connect()
                    sleep(3)
                    continue
            return con


    def save_to_db(self, df):
            """
            send data from csv to SQL database
            """
            con = self.engine.connect()
            con = self.pandas_to_sql(df=df, con=con)
            con.close()

            
    def create_pubsub_message_data(self, new_data):
        for transaction_id, download_id in new_data[['transaction_id', 'download_id']].values:
            pubsub_message = {
                'transaction_id': transaction_id,
                'download_id': download_id
            }
            pubsub_message_validated = transaction_id_download_id.parse_obj(pubsub_message)
            return pubsub_message_validated



    def download_and_save(self, data_handler):
        ''' 
        download, prepares, and saves the csv data from the landreg website
        update the SQL table which stores the date of data downloaded with current date
        '''
        while True:
            try:
                new_data = data_handler.get(nrows=Config.LR_DOWNLOAD_CONFIG['new_data_batchsize'])
            except StopIteration:
                break
            new_data = self.data_preparer.prepare_data(new_data)
            self.save_to_db(new_data)
            pubsub_message = self.create_pubsub_message_data(new_data)
        self.update_download_date()








            




