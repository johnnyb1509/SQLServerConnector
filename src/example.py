import os
import pandas as pd
import numpy as np 
from .utils import defineLoguru
from .SQLServerConnector import *
import argparse

parser = argparse.ArgumentParser(description='TESTING MODULE')

parser.add_argument('--dbServer', type = str, required=True)
parser.add_argument('--dbName', type = str, required=True)
parser.add_argument('--dbUser', type = str, required=True)
parser.add_argument('--dbPass', type = str, required=True)

args = parser.parse_args()

server = args.dbServer
username = args.dbUser
password = args.dbPass
database = args.dbName
driver = '{ODBC Driver 17 for SQL Server}'

logging = defineLoguru()

logging.info(f"SERVER: {server}")
logging.info(f"USERNAME: {username}")
logging.info(f"PWD: {password}")
logging.info(f"DRIVER: {driver}")

@logging.catch
def test():
    dbModule = dbJob(server, database, username, password, driver)
    logging.info(f"LIST OF TABLE {dbModule.tableInDB}")
    return

if __name__ == "__main__":
    test()