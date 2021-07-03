# import sqlalchemy
from sqlalchemy import create_engine
from pprint import pprint




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




databaseName = 'new_db'
username = 'SA'
password = 'Passw0rd2018'
server = 'localhost,1433'
driver= 'ODBC Driver 17 for SQL Server'
CONNECTION_STRING = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+databaseName+';UID='+username+';PWD='+ password

#Create/Open a Connection to Microsoft's SQL Server
db = pyodbc.connect(CONNECTION_STRING)
#create cursor
c = db.cursor()

# for driver in pyodbc.drivers():
#   print(driver)

# c.execute("DROP TABLE staff")
# c.commit()
# res = c.execute("SELECT TOP 5 * FROM staff")

# for x in res :
#   print(x)

c.execute("""
              DROP TABLE IF EXISTS candidate;
              CREATE TABLE candidate(
                      candidate_id int IDENTITY(1,1) PRIMARY KEY,
                      candidate_name VARCHAR(100),
                      gender VARCHAR(10),
                      dob DATETIME,
                      email VARCHAR(MAX),
                      city VARCHAR(MAX),
                      address VARCHAR(MAX),
                      postcode VARCHAR(10),
                      phone_number VARCHAR(15),
                      uni_name VARCHAR(MAX),
                      degree_result VARCHAR(10),
                      staff_id INT
                      )
              
              """)
c.commit()

# c.execute("DROP TABLE candidate")
c.commit()