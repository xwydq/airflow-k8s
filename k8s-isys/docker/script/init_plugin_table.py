import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

db_conn = os.environ['AIRFLOW_SQLCONN']
engine = create_engine(db_conn)
Session = sessionmaker(bind=engine)
session = Session()

sql_file = open('/script/plugin_table.sql','r')
sql_command = ''

for line in sql_file:
    if not line.startswith('--') and line.strip('\n'):
        sql_command += line.strip('\n')
        if sql_command.endswith(';'):
            print(sql_command)
            try:
                session.execute(text(sql_command))
                session.commit()
            finally:
                sql_command = ''