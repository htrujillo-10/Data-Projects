import sqlite3
import pandas as pd 
conn = sqlite3.connect("STAFF.db")

table_name = "INSTRUCTOR"
attribute_list = ["ID","FNAME","LNAME","CITY","CCODE"]
file_path = 'INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

df.to_sql(table_name, conn, if_exists = "replace", index = False)
print("table is ready")

query_statement = f"select * from {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"select FNAME from {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"select count(*) from {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

data_dict = {"ID":[100],"FNAME":["Jhon"],"LNAME":["Doe"],"CITY":["Paris"],"CCODE":["FR"]}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists = "append", index = False)
print("Data appended successfully")
query_statement = f"select * from {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

table_name2 = "Departments"
attribute_list2 = ["DEPT_ID","DEP_NAME","MANAGER_ID","LOC_ID"]
file_path2 = 'Departments.csv' 
df2 = pd.read_csv(file_path2, names = attribute_list2)

df2.to_sql(table_name2, conn, if_exists = "replace", index = False)
print("Table 2 is ready")

data_dict2 = {"DEPT_ID":[9],"DEP_NAME":["Quality Assurance"],"MANAGER_ID":[30010],"LOC_ID":["L0010"]}
data_append2 = pd.DataFrame(data_dict2)
data_append2.to_sql(table_name2, conn, if_exists = "append", index = False)
print("Data appendede successfully")

query_statement = f"select * from {table_name2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"select DEP_NAME from {table_name2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

query_statement = f"select count(*) from {table_name2}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()