# coding: utf8 
from __future__ import absolute_import, unicode_literals

import re
import json
from datetime import datetime, timedelta
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, BigInteger, Float)


Base = declarative_base()

DEFAULT_CONF = '{"tasks": []}'
reo_name = re.compile(r'[0-9a-z_]\w*$', re.I)

class DagxDag(Base):
    __tablename__ = "dagx_dag"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    uuid = Column(String(128), nullable=False)
    name = Column(String(128), nullable=False)
    owner = Column(String(64), default='admin')
    action = Column(String(64), default='')
    version = Column(Integer, default=0)
    crond = Column(String(64), default='')
    start_date = Column(DateTime)
    end_date = Column(DateTime)

    retries = Column(Integer, default=2)
    retry_delay_minutes = Column(Integer, default=3)
    email_on_failure = Column(Boolean, default=1)
    email_on_retry = Column(Boolean, default=0)
    depends_on_past = Column(Boolean, default=1)
    concurrency = Column(Integer, default=16)
    max_active_runs = Column(Integer, default=16)
    skip_dag_not_latest = Column(Boolean, default=1)
    emails = Column(String(500), default='xuwy@isyscore.com')

    conf = Column(Text, default=DEFAULT_CONF)
    is_delete = Column(Boolean, default=0)
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, 
            onupdate=datetime.now)

    def fmt_date(self, d):
        return d.strftime('%Y-%m-%d %H:%M:%S')

    def to_json(self):
        now = datetime.now()
        return {'uuid': self.uuid or '',
                'name': self.name or 'new', 
                'owner': self.owner or 'admin', 
                'action': self.action or '', 
                'version': self.version or 0,
                'crond': self.crond or '*/10 * * * *',
                'start_date': str(self.start_date or self.fmt_date(now)),
                'end_date': str(self.end_date or
                        self.fmt_date(now + timedelta(days=10))),

                'retries': self.retries or 2,
                'retry_delay_minutes': self.retry_delay_minutes or 3,
                'email_on_failure': 'true' if bool(self.email_on_failure) else 'false',
                'email_on_retry': 'true' if bool(self.email_on_retry) else 'false',
                'depends_on_past': 'true' if bool(self.depends_on_past) else 'false',
                'concurrency': self.concurrency or 16,
                'max_active_runs': self.max_active_runs or 16,
                'skip_dag_not_latest': 'true' if bool(self.skip_dag_not_latest) else 'false',
                'emails': self.emails or 'xuwy@isyscore.com',

                'conf': json.loads(self.conf or DEFAULT_CONF, strict=False),
                'create_time': str(self.create_time or ''), 
                'update_time': str(self.update_time or '')}

    def from_json(self, obj):
        self.name = obj['name']
        if not reo_name.match(self.name):
            return 'invalid dag name {}'.format(obj['name'])
        self.owner = obj.get('owner', '')
        self.action = obj.get('action', '')
        self.version = int(obj.get('version', '') or 0)
        self.crond = obj.get('crond', '')
        if obj.get('start_date', ''):
            self.start_date = datetime.strptime(obj['start_date'], 
                    '%Y-%m-%d %H:%M:%S')
        if obj.get('end_date', ''):
            self.end_date = datetime.strptime(obj['end_date'], 
                    '%Y-%m-%d %H:%M:%S')

        self.retries = int(obj.get('retries', '') or 2)
        self.retry_delay_minutes = int(obj.get('retry_delay_minutes', '') or 3)
        self.email_on_failure = True if obj.get('email_on_failure', 'true') == 'true' else False
        self.email_on_retry = True if obj.get('email_on_retry', 'false') == 'true' else False
        self.depends_on_past = True if obj.get('depends_on_past', 'true') == 'true' else False
        self.concurrency = int(obj.get('concurrency', '') or 16)
        self.max_active_runs = int(obj.get('max_active_runs', '') or 16)
        self.skip_dag_not_latest = True if obj.get('skip_dag_not_latest', 'true') == 'true' else False
        self.emails = obj.get('emails', 'xuwy@isyscore.com')

        for t in obj['conf']['tasks']:
            if not reo_name.match(t['name']):
                return 'invalid task name {}'.format(t['name'])
        self.conf = json.dumps(obj['conf'])
        return ''


class DagxConf2Py(Base):
    __tablename__ = "dagx_conf2py"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    uuid = Column(String(128), nullable=False)
    status = Column(Boolean, default=0)
    desc = Column(Text)
    scan_time = Column(DateTime)

