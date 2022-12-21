from config import Configuration as Config
from SQLhandler import RawSQLHandler
from data_downloader import DownloadHandler
from date_checker import DateChecker
from dateutil import parser
from data_processor import DataPreparer

class Main:
    """
    Handles the running of the lr_download microservice.
    The microservice downloads land reg data from the gov.uk website
    """

    def __init__(self):
        self.request_handler = RawSQLHandler()
        self.data_preparer = DataPreparer()
        self.data_downloader = DownloadHandler(file_type="monthly") #object


    def main(self):
        """
        main method downloads and saves the data.
        """

        condition = self.request_handler.check_for_website_update()
        
        if condition == True:
            print("The www.gov.uk website has uploaded new land reg data since we the last time that we downloaded it")
            print("pulling new landreg data...")
            self.request_handler.download_and_save(self.data_downloader)
        else:
            print("The www.gov.uk website has not uploaded new data since we the last time that we downloaded it.")
        
    

if __name__ == "__main__":
    main = Main()
    main.main()