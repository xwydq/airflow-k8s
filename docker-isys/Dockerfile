##############################################
# 基于centos7构建airflow docker 环境
##############################################
FROM centos:7
MAINTAINER xuwy

COPY CentOS-Base.repo /etc/yum.repos.d/

COPY Python-3.7.0.tgz /Python-3.7.0.tgz
COPY oracle-instantclient12.1 /oracle-instantclient12.1

COPY requirement.txt /requirement.txt

ARG AIRFLOW_VERSION=1.10.3
ENV AIRFLOW_HOME=/home/airflow/airflow

#ENV MYSQL_HOST=10.30.30.27
#ENV MYSQL_PORT=63306
#ENV MYSQL_USER=isyscore
#ENV MYSQL_PASSWORD=Isysc0re
#ENV MYSQL_DB=Airflow
#ENV AIRFLOW_SQLCONN=mysql://$MYSQL_USER:$MYSQL_PASSWORD@$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB?charset=utf8


RUN set -ex \
    # 预安装所需组件
    && echo "nameserver 144.144.144.144" >> /etc/resolv.conf \
    #&& wget -O /etc/yum.repos.d/CentOS-Base.repo http://mirrors.aliyun.com/repo/Centos-7.repo  \
    && yum install -y wget tar libffi-devel zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gcc make initscripts \
    && yum install -y mysql-devel postgresql-devel libsasl2-devel openldap-devel gcc-c++ libaio \
    # 安装Python-3.7.0
    #&& wget https://www.python.org/ftp/python/3.7.0/Python-3.7.0.tgz \
    && tar -zxvf Python-3.7.0.tgz \
    && cd Python-3.7.0 \
    && ./configure prefix=/usr/local/python3 \
    && make \
    && make install \
    && make clean \
    && cd / \
    && rm -rf /Python-3.7.0* \
    && yum install -y epel-release \
    && yum install -y python-pip \
    # 备份旧版本python
    && mv /usr/bin/python /usr/bin/python27 \
    && mv /usr/bin/pip /usr/bin/pip-python2.7 \
    # 配置默认为python3
    && ln -s /usr/local/python3/bin/python3.7 /usr/bin/python \
    && ln -s /usr/local/python3/bin/pip3 /usr/bin/pip \
    # 修复因修改python版本导致yum失效问题
    && sed -i "s#/usr/bin/python#/usr/bin/python2.7#" /usr/bin/yum \
    && sed -i "s#/usr/bin/python#/usr/bin/python2.7#" /usr/libexec/urlgrabber-ext-down \
    && yum install -y deltarpm \
    # 安装 oracle-instantclient
    && rpm -ivh /oracle-instantclient12.1/oracle-instantclient12.1-basic-12.1.0.2.0-1.x86_64.rpm \
    && rpm -ivh /oracle-instantclient12.1/oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && echo 'export ORACLE_HOME=/usr/lib/oracle/12.1/client64' >> /etc/profile \
    && echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME:$ORACLE_HOME/lib' >> /etc/profile \
    && echo 'export PATH=$PATH:/usr/local/python3/bin' >> /root/.bashrc \
    && echo 'export ORACLE_HOME=/usr/lib/oracle/12.1/client64' >> /root/.bashrc \
    && echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME:$ORACLE_HOME/lib' >> /root/.bashrc \
    && echo 'export PATH=$PATH:/usr/local/python3/bin' >> /etc/profile \
    && source /etc/profile \
    # 修改系统时区为东八区
    && rm -rf /etc/localtime \
    && ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && yum install -y vim \
    # 安装定时任务组件
    && yum -y install cronie \
    # 支持中文
    && yum install kde-l10n-Chinese -y \
    && localedef -c -f UTF-8 -i zh_CN zh_CN.utf8 \
    # 更新pip版本
    && pip install --upgrade pip \
    # 安装airflow及依赖
    && mkdir -p ~/.pip/  \
    && echo '[global]' >> ~/.pip/pip.conf \
    && echo 'index-url = http://mirrors.aliyun.com/pypi/simple ' >> ~/.pip/pip.conf \
    && echo '[install]' >> ~/.pip/pip.conf \
    && echo 'trusted-host=mirrors.aliyun.com' >> ~/.pip/pip.conf \
    && cd / \
    && pip install -r requirement.txt \
    && rm -rf /Python-3.7.0.tgz \
    && rm -rf /oracle-instantclient12.1

ENV LC_ALL zh_CN.UTF-8

COPY airflow /airflow
COPY plugins /plugins
COPY script /script
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY dags/tools ${AIRFLOW_HOME}/dags/tools

RUN set -ex \
    && echo 'export PATH=$PATH:/usr/local/python3/bin' >> /root/.bashrc \
    && echo 'export ORACLE_HOME=/usr/lib/oracle/12.1/client64' >> /root/.bashrc \
    && echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME:$ORACLE_HOME/lib' >> /root/.bashrc \
    && ln -s /usr/local/python3/bin/airflow /usr/bin/airflow \
    && ln -s /usr/local/python3/bin/gunicorn /usr/bin/gunicorn \
    && cd / \
    && mkdir -p /home/airflow \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && rm -rf /usr/local/python3/lib/python3.7/site-packages/airflow \
    && cp -r /airflow /usr/local/python3/lib/python3.7/site-packages/ \
    && cp -r /plugins ${AIRFLOW_HOME} \
    && chmod +x /script/entrypoint.sh

EXPOSE 8080 5555 8793
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/script/entrypoint.sh"]
CMD ["webserver"]
