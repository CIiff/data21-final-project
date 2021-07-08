import sqlalchemy
from sqlalchemy import create_engine
from pprint import pprint
import urllib


import pyodbc

# server = 'localhost,1433'
# database = 'new_db'
# user = 'SA'
# password = 'Passw0rd2018'
# driver = 'SQL+Server'
# #driver = 'ODBC Driver 17 for SQL Server'

# engine = create_engine(
#     f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")

# connection = engine.connect()


# # res = engine.execute("SELECT TOP 3 * FROM candidate")
# # for x in res:
# #   pprint (list(x))

# # engine.execute(" CREATE TABLE [IF NOT EXISTS] testing(staff_id int PRIMARY KEY AUTO_INCREMENT,staff_name VARCHAR(130),department VARCHAR(130))")

# engine.execute("DROP TABLE IF EXISTS testing")
# engine.execute("CREATE TABLE testing (staff_id int PRIMARY KEY,staff_name VARCHAR(130),department VARCHAR(130))")

# for x in engine.execute("SELECT * FROM staff"):
#   print(x)


databaseName = 'Data21Final'
username = 'SA'
password = 'Passw0rd2018'
server = 'localhost'
port = '1433'
driver = 'ODBC Driver 17 for SQL Server'
# CONNECTION_STRING = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+databaseName+';UID='+username+';PWD='+ password

# #Create/Open a Connection to Microsoft's SQL Server
# db = pyodbc.connect(CONNECTION_STRING)
# #create cursor
# c = db.cursor()

# dbs = [db[0] for db in c.execute('SELECT name FROM sys.databases')]
# print(dbs)

# # for db in dbs:
# #         print(db)

# # for driver in pyodbc.drivers():
# #   print(driver)

# # c.execute("DROP TABLE staff")
# # c.commit()
# # res = c.execute("SELECT TOP 5 * FROM staff")

# # for x in res :
# #   print(x)

connect_string = urllib.parse.quote_plus(
    f'DRIVER={driver};Server={server},{port};Database={databaseName};UID=SA;PWD=Passw0rd2018')
engine = sqlalchemy.create_engine(
    f'mssql+pyodbc:///?odbc_connect={connect_string}', fast_executemany=True)
db = engine.connect()


dbs = [db[0] for db in engine.execute('SELECT name FROM sys.databases')]
print(dbs)

tables = [table[0] for table in engine.execute(
    """SELECT *FROM SYSOBJECTS WHERE xtype = 'U';""")]

print(tables)
