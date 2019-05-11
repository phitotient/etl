import sqlalchemy as db
import json
class db_connect:

    def __init__(self):

        with open('config.json') as json_file:
            self.properties = json.load(json_file)

        self.connection_string=self.properties["connection_string"]
        self.engine = db.create_engine(self.connection_string)
        self.connection = self.engine.connect()
        self.meta = db.MetaData()

    def create_sql(self,table_name):

        file = open("templates/extract_data.sql")
        f_read= file.read()
        fs = f_read.replace("{table_name}",table_name)
        return fs

    def fetch_columns(self,table_name):

        etl = db.Table(table_name, self.meta, autoload=True, autoload_with=self.engine)
        columns=etl.columns.keys()
        return columns

    def execute_query(self,fs):

        query=db.text(fs)
        a = self.engine.execute(query)
        res = a.fetchall()
        return res

