import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

db_host = os.environ['MYSQL_HOST']
db_port = os.environ['MYSQL_PORT']
db_user = os.environ['MYSQL_USER']
db_pwd = os.environ['MYSQL_PASSWORD']
db_conn = os.environ['AIRFLOW_SQLCONN']

engine = create_engine('mysql://{0}:{1}@{2}:{3}/'.format(db_user, db_pwd, db_host, db_port))
Session = sessionmaker(bind=engine)
session = Session()

sql_file = open('/script/create_db_env.sql','r')
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

