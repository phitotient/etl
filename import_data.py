from db_connect import db_connect
from pandas_helper import pandas_helper
import json

class import_data:

    def __init__(self):

        with open('config.json') as json_file:
            properties = json.load(json_file)
        self.dc=db_connect()
        self.ph=pandas_helper()
        self.table_name=properties["table_name"]
        self.columns_import=properties["columns_import"]

    def download(self):

        sql=self.dc.create_sql(self.table_name)
        columns=self.dc.fetch_columns(self.table_name)
        response=self.dc.execute_query(sql)
        return response,columns

    def create_dataframe_write_csv(self):
        response,columns=self.download()
        dataframe=self.ph.create_dataframe(response,columns )
        self.ph.write_to_csv(dataframe,self.table_name)