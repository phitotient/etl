import pandas as pd


class pandas_helper:

    def create_dataframe(self,response,columns):
        dataframe=pd.DataFrame(response,columns=columns)
        return dataframe

    def write_to_csv(self,dataframe,table_name):
        dataframe.to_csv("output/"+table_name, sep=',', encoding='utf-8',compression="gzip")
        print("File Created Successfully !!!")
