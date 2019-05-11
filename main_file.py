from import_data import  import_data
from celery import Celery
from celery import task


celery = Celery('data_extract',
                 backend='rpc://',
                 broker='pyamqp://guest@localhost//')




im=import_data()

@task
def run_test():
    im.create_dataframe_write_csv()



