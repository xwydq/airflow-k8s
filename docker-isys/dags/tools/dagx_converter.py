#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from datetime import datetime, date, time, timedelta

import airflow
import requests
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow import settings
from airflow.models import Variable, TaskInstance, DagRun
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
# from airflow.operators.sensors import TimeSensor, TimeDeltaSensor
from airflow.utils.email import send_email


TI = TaskInstance


def send_alert_email(mark, context, raise_exception=False):
    title = "[%s] %s@%s: Airflow alert" % (mark, context["dag"].dag_id, context["ti"].hostname)
    body = (
        "Log: <a href='{ti.log_url}'>Link</a><br>"
        "Host: {ti.hostname}<br>"
        "Log file: {ti.log_filepath}<br>"
    ).format(**{"ti": context["ti"]})
    try:
        send_email(context["task"].email, title, body)
    except Exception as e:
        logging.exception("send email failed")
        if raise_exception:
            raise e


def get_default_params():
    now = datetime.now()
    last_month = now + relativedelta(months=-1)
    return {
        'yesterday': (now - timedelta(days=1)).strftime("%Y-%m-%d"),
        'today': now.strftime("%Y-%m-%d"),
        'this_month': now.strftime("%Y-%m"),
        'this_month_first_day': (now + relativedelta(day=1)).strftime("%Y-%m-%d"),
        'this_month_last_day': (now + relativedelta(day=31)).strftime("%Y-%m-%d"),
        'last_month': last_month.strftime("%Y-%m"),
        'last_month_first_day': (last_month + relativedelta(day=1)).strftime("%Y-%m-%d"),
        'last_month_last_day': (last_month + relativedelta(day=31)).strftime("%Y-%m-%d"),
    }


default_params = get_default_params()


default_args = {
    'owner': 'admin',
    'start_date': airflow.utils.dates.days_ago(1),
    'end_date': datetime.strptime("2029-09-21 14:12:07", "%Y-%m-%d %H:%M:%S"),
    'email': ["xuwy@isyscore.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}


dag = DAG(
    'dagx_converter', default_args=default_args, params=default_params, concurrency=16, max_active_runs=16, schedule_interval='*/1 * * * *')

_ = {}


_["dagx_convert_sh"] = BashOperator(
    task_id='dagx_convert_sh',

    bash_command=r'''$AIRFLOW_HOME/dags/tools/dagx_render.sh  ''',

    priority_weight=0,
    queue='default',
    pool=None,
    dag=dag,
    )

_["dagx_convert_sh"].category = {
    "name": r'''default''',
    "fgcolor": r'''#f0ede4''',
    "order": 0,
}

