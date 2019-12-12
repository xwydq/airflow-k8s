import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
user = PasswordUser(models.User())
user.username = 'zlj'
user.email = 'zlj@isyscore.com'
user.password = 'zlj'
user.superuser = True
session = settings.Session()
session.add(user)
session.commit()
session.close()