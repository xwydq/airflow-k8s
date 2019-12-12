# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from flask import (
    g, Blueprint, jsonify, request, url_for
)

import airflow.api
from airflow.api.common.experimental import delete_dag as delete
from airflow.api.common.experimental import pool as pool_api
from airflow.api.common.experimental import trigger_dag as trigger
from airflow.api.common.experimental.get_dag_runs import get_dag_runs
from airflow.api.common.experimental.get_task import get_task
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.api.common.experimental.get_code import get_code
from airflow.api.common.experimental.get_dag_run_state import get_dag_run_state
from airflow.exceptions import AirflowException
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.app import csrf
from airflow import models
from airflow.utils.db import create_session

_log = LoggingMixin().log

requires_authentication = airflow.api.api_auth.requires_authentication

api_experimental = Blueprint('api_experimental', __name__)


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['POST'])
@requires_authentication
def trigger_dag(dag_id):
    """
    Trigger a new dag run for a Dag with an execution date of now unless
    specified in the data.
    """
    data = request.get_json(force=True)

    run_id = None
    if 'run_id' in data:
        run_id = data['run_id']

    conf = None
    if 'conf' in data:
        conf = data['conf']

    execution_date = None
    if 'execution_date' in data and data['execution_date'] is not None:
        execution_date = data['execution_date']

        # Convert string datetime into actual datetime
        try:
            execution_date = timezone.parse(execution_date)
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                    execution_date))
            _log.info(error_message)
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

    try:
        dr = trigger.trigger_dag(dag_id, run_id, conf, execution_date)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    if getattr(g, 'user', None):
        _log.info("User %s created %s", g.user, dr)

    response = jsonify(message="Created {}".format(dr))
    return response


@csrf.exempt
@api_experimental.route('/dags/<string:dag_id>', methods=['DELETE'])
@requires_authentication
def delete_dag(dag_id):
    """
    Delete all DB records related to the specified Dag.
    """
    try:
        count = delete.delete_dag(dag_id)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    return jsonify(message="Removed {} record(s)".format(count), count=count)


@api_experimental.route('/dags/<string:dag_id>/dag_runs', methods=['GET'])
@requires_authentication
def dag_runs(dag_id):
    """
    Returns a list of Dag Runs for a specific DAG ID.
    :query param state: a query string parameter '?state=queued|running|success...'
    :param dag_id: String identifier of a DAG
    :return: List of DAG runs of a DAG with requested state,
    or all runs if the state is not specified
    """
    try:
        state = request.args.get('state')
        dagruns = get_dag_runs(dag_id, state, run_url_route='airflow.graph')
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = 400
        return response

    return jsonify(dagruns)


@api_experimental.route('/test', methods=['GET'])
@requires_authentication
def test():
    return jsonify(status='OK')


@api_experimental.route('/dags/<string:dag_id>/code', methods=['GET'])
@requires_authentication
def get_dag_code(dag_id):
    """Return python code of a given dag_id."""
    try:
        return get_code(dag_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response


@api_experimental.route('/dags/<string:dag_id>/tasks/<string:task_id>', methods=['GET'])
@requires_authentication
def task_info(dag_id, task_id):
    """Returns a JSON with a task's public instance variables. """
    try:
        info = get_task(dag_id, task_id)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


# ToDo: Shouldn't this be a PUT method?
@api_experimental.route('/dags/<string:dag_id>/paused/<string:paused>', methods=['GET'])
@requires_authentication
def dag_paused(dag_id, paused):
    """(Un)pauses a dag"""

    DagModel = models.DagModel
    with create_session() as session:
        orm_dag = (
            session.query(DagModel)
                   .filter(DagModel.dag_id == dag_id).first()
        )
        if paused == 'true':
            orm_dag.is_paused = True
        else:
            orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()

    return jsonify({'response': 'ok'})


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>/tasks/<string:task_id>',
    methods=['GET'])
@requires_authentication
def task_instance_info(dag_id, execution_date, task_id):
    """
    Returns a JSON with a task instance's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """

    # Convert string datetime into actual datetime
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        info = get_task_instance(dag_id, task_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    # JSONify and return.
    fields = {k: str(v)
              for k, v in vars(info).items()
              if not k.startswith('_')}
    return jsonify(fields)


@api_experimental.route(
    '/dags/<string:dag_id>/dag_runs/<string:execution_date>',
    methods=['GET'])
@requires_authentication
def dag_run_status(dag_id, execution_date):
    """
    Returns a JSON with a dag_run's public instance variables.
    The format for the exec_date is expected to be
    "YYYY-mm-DDTHH:MM:SS", for example: "2016-11-16T11:34:15". This will
    of course need to have been encoded for URL in the request.
    """

    # Convert string datetime into actual datetime
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                execution_date))
        _log.info(error_message)
        response = jsonify({'error': error_message})
        response.status_code = 400

        return response

    try:
        info = get_dag_run_state(dag_id, execution_date)
    except AirflowException as err:
        _log.info(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response

    return jsonify(info)


@api_experimental.route('/latest_runs', methods=['GET'])
@requires_authentication
def latest_dag_runs():
    """Returns the latest DagRun for each DAG formatted for the UI. """
    from airflow.models import DagRun
    dagruns = DagRun.get_latest_runs()
    payload = []
    for dagrun in dagruns:
        if dagrun.execution_date:
            payload.append({
                'dag_id': dagrun.dag_id,
                'execution_date': dagrun.execution_date.isoformat(),
                'start_date': ((dagrun.start_date or '') and
                               dagrun.start_date.isoformat()),
                'dag_run_url': url_for('airflow.graph', dag_id=dagrun.dag_id,
                                       execution_date=dagrun.execution_date)
            })
    return jsonify(items=payload)  # old flask versions dont support jsonifying arrays


@api_experimental.route('/pools/<string:name>', methods=['GET'])
@requires_authentication
def get_pool(name):
    """Get pool by a given name."""
    try:
        pool = pool_api.get_pool(name=name)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@api_experimental.route('/pools', methods=['GET'])
@requires_authentication
def get_pools():
    """Get all pools."""
    try:
        pools = pool_api.get_pools()
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify([p.to_json() for p in pools])


@csrf.exempt
@api_experimental.route('/pools', methods=['POST'])
@requires_authentication
def create_pool():
    """Create a pool."""
    params = request.get_json(force=True)
    try:
        pool = pool_api.create_pool(**params)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())


@csrf.exempt
@api_experimental.route('/pools/<string:name>', methods=['DELETE'])
@requires_authentication
def delete_pool(name):
    """Delete pool."""
    try:
        pool = pool_api.delete_pool(name=name)
    except AirflowException as err:
        _log.error(err)
        response = jsonify(error="{}".format(err))
        response.status_code = err.status_code
        return response
    else:
        return jsonify(pool.to_json())



import pandas as pd
from sqlalchemy import create_engine, text
import json
from airflow import configuration
import datetime

engine = create_engine(configuration.get('core', 'sql_alchemy_conn'))

@api_experimental.route('/dags_table', methods=['GET'])
def get_dags_table():
    """
    Returns a list of Dag Runs for a specific date.
    :query param start_date/end_date
    :return: 返回指定日期范围的dag run 信息,
    """

    start_date, end_date = request.args.get('start_date'), request.args.get('end_date')
    page, size = request.args.get('page'), request.args.get('size')
    if page is not None and size is not None:
        try:
            page = int(page)
            size = int(size)
        except ValueError:
            response = jsonify({'error': 'page, size could not be identified'})
            response.status_code = 400
            return response
    if page is None or size is None:
        response = jsonify({'error': 'page, size could not be identified'})
        response.status_code = 400
        return response
    if start_date is not None and end_date is not None:
        # Convert string datetime into actual datetime
        try:
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            error_message = (
                'Given start_date = {} or end_date = {} could not be identified '
                'as a date. Example date format: 2019-11-16'.format(
                    start_date, end_date))
            response = jsonify({'error': error_message})
            response.status_code = 400
            return response

        sql_fmt = """
        select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
               t.dag_id,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
               TIMESTAMPDIFF(SECOND, t.start_date, t.end_date) duration_sec,
               t.state
        from dag_run t
        where t.start_date is not null
        and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') >= '{0}'
        and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') <= '{1}'
        """
        sql = sql_fmt.format(start_date, end_date)
        df = pd.read_sql(text(sql), con=engine)
        df.loc[pd.isnull(df['state']), 'state'] = 'failed'
        df.loc[~df['state'].isin(['running', 'success']), 'state'] = 'failed'

        sql_task = """
        select dag_id, count(distinct task_id) task_num from task_instance group by dag_id
        """
        task_num = pd.read_sql(text(sql_task), con=engine)
        res = pd.merge(df, task_num, how='left', on='dag_id')
        total_num = res.shape[0]
        res = res.iloc[((page - 1) * size):(page * size), ]
        response = jsonify({'total_num': total_num,
                            'data': json.loads(res.to_json(orient='records'))})
        return response

    else:
        response = jsonify({'error': 'request arg `start_date, end_date` not found'})
        response.status_code = 400
        return response


@api_experimental.route('/dags_daysum', methods=['GET'])
def get_dags_daysum():
    """
    :query param execution_date
    :return: 返回指定日期 dag 运行信息（成功失败数量）,
    """
    execution_date = request.args.get('execution_date')

    if execution_date is not None:
        # Convert string datetime into actual datetime
        try:
            execution_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2019-11-16'.format(
                    execution_date))
            response = jsonify({'error': error_message})
            response.status_code = 400
            return response

        sql_fmt = """
        select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
               t.dag_id,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
               TIMESTAMPDIFF(SECOND, t.start_date, t.end_date) duration_sec,
               t.state
        from dag_run t
        where t.start_date is not null
        and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') = '{}'
        """

        sql = sql_fmt.format(execution_date)
        df = pd.read_sql(text(sql), con=engine)
        df.loc[pd.isnull(df['state']), 'state'] = 'failed'
        df.loc[~df['state'].isin(['running', 'success']), 'state'] = 'failed'
        rest = df.groupby(['state'], as_index=False)["ymd"].agg({"num": "count"})
        res = json.loads(rest.to_json(orient='records'))
        return jsonify(res)

    else:
        response = jsonify({'error': 'request arg `execution_date` not found'})
        response.status_code = 400
        return response



@api_experimental.route('/dags_weeksum', methods=['GET'])
def get_dags_weeksum():
    """
    :query param execution_date
    :return: 返回指定日期最近一个星期 dag 运行信息（成功失败数量）,
    """
    execution_date = request.args.get('execution_date')
    # execution_date = '2019-10-22'
    if execution_date is not None:
        # Convert string datetime into actual datetime
        try:
            execution_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2019-11-16'.format(
                    execution_date))
            response = jsonify({'error': error_message})
            response.status_code = 400
            return response
        start_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d").date()
        start_date = start_date + datetime.timedelta(days=-7)
        start_date = start_date.strftime("%Y-%m-%d")

        sql_fmt = """
        select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
               t.dag_id,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
               TIMESTAMPDIFF(SECOND, t.start_date, t.end_date) duration_sec,
               t.state
        from dag_run t
        where t.start_date is not null
        and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') <= '{0}'
        and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') > '{1}'
        """
        sql = sql_fmt.format(execution_date, start_date)
        df = pd.read_sql(text(sql), con=engine)
        df.loc[pd.isnull(df['state']), 'state'] = 'failed'
        df.loc[~df['state'].isin(['running', 'success']), 'state'] = 'failed'
        rest = df.groupby(['state'], as_index=False)["ymd"].agg({"num": "count"})
        res = json.loads(rest.to_json(orient='records'))
        return jsonify(res)
    else:
        response = jsonify({'error': 'request arg `execution_date` not found'})
        response.status_code = 400
        return response


@api_experimental.route('/tasks_daysum', methods=['GET'])
def get_tasks_daysum():
    """
    :query param execution_date
    :return: 返回指定日期 task 运行信息（成功失败数量）,
    """
    execution_date = request.args.get('execution_date')

    if execution_date is not None:
        # Convert string datetime into actual datetime
        try:
            execution_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2019-11-16'.format(
                    execution_date))
            response = jsonify({'error': error_message})
            response.status_code = 400
            return response

        sql_fmt = """
        select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
               t.task_id, t.dag_id,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
               t.duration duration_sec,
               t.state
        from task_instance t
        where t.start_date is not null
        and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') = '{}'
        """

        sql = sql_fmt.format(execution_date)
        df = pd.read_sql(text(sql), con=engine)
        df.loc[pd.isnull(df['state']), 'state'] = 'failed'
        df.loc[~df['state'].isin(['running', 'success', 'queued', 'scheduled', 'skipped']), 'state'] = 'failed'
        rest = df.groupby(['state'], as_index=False)["ymd"].agg({"num": "count"})
        res = json.loads(rest.to_json(orient='records'))
        return jsonify(res)

    else:
        response = jsonify({'error': 'request arg `execution_date` not found'})
        response.status_code = 400
        return response



@api_experimental.route('/tasks_weeksum', methods=['GET'])
def get_tasks_weeksum():
    """
    :query param execution_date
    :return: 返回指定日期最近一个星期 task 运行信息（成功失败数量）,
    """
    execution_date = request.args.get('execution_date')
    # execution_date = '2019-10-22'
    if execution_date is not None:
        # Convert string datetime into actual datetime
        try:
            execution_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2019-11-16'.format(
                    execution_date))
            response = jsonify({'error': error_message})
            response.status_code = 400
            return response

        start_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d").date()
        start_date = start_date + datetime.timedelta(days=-7)
        start_date = start_date.strftime("%Y-%m-%d")

        sql_fmt = """
        select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
               t.task_id, t.dag_id,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
               DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
               t.duration duration_sec,
               t.state
        from task_instance t
        where t.start_date is not null
        and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') <= '{0}'
        and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') > '{1}'
        """

        sql = sql_fmt.format(execution_date, start_date)
        df = pd.read_sql(text(sql), con=engine)
        df.loc[pd.isnull(df['state']), 'state'] = 'failed'
        df.loc[~df['state'].isin(['running', 'success', 'queued', 'scheduled', 'skipped']), 'state'] = 'failed'
        rest = df.groupby(['state'], as_index=False)["ymd"].agg({"num": "count"})
        res = json.loads(rest.to_json(orient='records'))
        return jsonify(res)

    else:
        response = jsonify({'error': 'request arg `execution_date` not found'})
        response.status_code = 400
        return response




#################
@api_experimental.route('/tasks_table', methods=['GET'])
def get_tasks_table():
    """
    Returns a list of task instance for a specific date.
    :query param start_date/end_date
    :return: 返回指定日期范围的 task实例 信息,
    """
    sql_fmt = """
    select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
           t.task_id, t.dag_id,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
           t.duration duration_sec,
           t.state
    from task_instance t
    where t.start_date is not null
    and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') >= '{0}'
    and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') <= '{1}'
    """
    sql = sql_fmt.format(request.args.get('start_date'), request.args.get('end_date'))
    df = pd.read_sql(text(sql), con=engine)
    res = json.loads(df.to_json(orient='records'))
    return jsonify(res)



@api_experimental.route('/dags_dayinfo', methods=['GET'])
def get_dags_dayinfo():
    """
    :query param execution_date
    :return: 返回指定日期 dag 运行信息（成功失败数量；运行时长）,
    """
    execution_date = request.args.get('execution_date')
    sql_fmt = """
    select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
           t.dag_id,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
           TIMESTAMPDIFF(SECOND, t.start_date, t.end_date) duration_sec,
           t.state
    from dag_run t
    where t.start_date is not null
    and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') = '{}'
    """

    sql = sql_fmt.format(execution_date)
    df = pd.read_sql(text(sql), con=engine)
    df = df.groupby(['dag_id', 'state'], as_index=False)["duration_sec"].agg({"num": "count", "duration_sec": "mean"})
    rest = df.groupby('dag_id').apply(lambda x:
                                      {'num': dict(zip(x['state'], x['num'])),
                                       'duration_sec': dict(zip(x['state'], x['duration_sec']))}).to_json(
        orient='index')
    print(rest)
    res = json.loads(rest)
    return jsonify(res)



@api_experimental.route('/dags_weekinfo', methods=['GET'])
def get_dags_weekinfo():
    """
    :query param execution_date
    :return: 返回指定日期最近一个星期 dag 运行信息（成功失败数量；运行时长）,
    """
    execution_date = request.args.get('execution_date')
    # execution_date = '2019-10-22'
    start_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d").date()
    start_date = start_date + datetime.timedelta(days=-7)
    start_date = start_date.strftime("%Y-%m-%d")

    sql_fmt = """
    select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
           t.dag_id,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
           TIMESTAMPDIFF(SECOND, t.start_date, t.end_date) duration_sec,
           t.state
    from dag_run t
    where t.start_date is not null
    and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') <= '{0}'
    and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') > '{1}'
    """

    sql = sql_fmt.format(execution_date, start_date)
    df = pd.read_sql(text(sql), con=engine)
    df = df.groupby(['dag_id', 'state'], as_index=False)["duration_sec"].agg({"num": "count", "duration_sec": "mean"})
    rest = df.groupby('dag_id').apply(lambda x:
                                      {'num': dict(zip(x['state'], x['num'])),
                                       'duration_sec': dict(zip(x['state'], x['duration_sec']))}).to_json(
        orient='index')
    print(rest)
    res = json.loads(rest)
    return jsonify(res)


@api_experimental.route('/tasks_dayinfo', methods=['GET'])
def get_tasks_dayinfo():
    """
    :query param execution_date
    :return: 返回指定日期 task 运行信息（成功失败数量；运行时长）,
    """
    execution_date = request.args.get('execution_date')
    sql_fmt = """
    select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
           t.task_id, t.dag_id,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
           t.duration duration_sec,
           t.state
    from task_instance t
    where t.start_date is not null
    and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') = '{}'
    """

    sql = sql_fmt.format(execution_date)
    df = pd.read_sql(text(sql), con=engine)

    rest = df.groupby(['task_id', 'state'], as_index=False)["duration_sec"].agg({"num": "count", "duration_sec": "mean"})
    rest = rest.groupby('task_id').apply(lambda x:
                                      {'num': dict(zip(x['state'], x['num'])),
                                       'duration_sec': dict(zip(x['state'], x['duration_sec']))}).to_json(
        orient='index')
    res = json.loads(rest)
    return jsonify(res)



@api_experimental.route('/tasks_weekinfo', methods=['GET'])
def get_tasks_weekinfo():
    """
    :query param execution_date
    :return: 返回指定日期最近一个星期 task 运行信息（成功失败数量；运行时长）,
    """
    execution_date = request.args.get('execution_date')
    # execution_date = '2019-10-22'
    start_date = datetime.datetime.strptime(execution_date, "%Y-%m-%d").date()
    start_date = start_date + datetime.timedelta(days=-7)
    start_date = start_date.strftime("%Y-%m-%d")

    sql_fmt = """
    select DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') as ymd,
           t.task_id, t.dag_id,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d %H:%i:%S') as start_date,
           DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.end_date), '%Y-%m-%d %H:%i:%S') as end_date,
           t.duration duration_sec,
           t.state
    from task_instance t
    where t.start_date is not null
    and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') <= '{0}'
    and DATE_FORMAT(TIMESTAMPADD(HOUR, -8, t.start_date), '%Y-%m-%d') > '{1}'
    """

    sql = sql_fmt.format(execution_date, start_date)
    df = pd.read_sql(text(sql), con=engine)
    df = df.groupby(['task_id', 'state'], as_index=False)["duration_sec"].agg({"num": "count", "duration_sec": "mean"})
    rest = df.groupby('task_id').apply(lambda x:
                                      {'num': dict(zip(x['state'], x['num'])),
                                       'duration_sec': dict(zip(x['state'], x['duration_sec']))}).to_json(
        orient='index')
    print(rest)
    res = json.loads(rest)
    return jsonify(res)
