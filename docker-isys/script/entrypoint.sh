#!/bin/bash

chmod +x $AIRFLOW_HOME/dags/tools/dagx_render.sh

: "${MYSQL_HOST:="mysql-service"}"
: "${MYSQL_PORT:="3306"}"
: "${MYSQL_USER:="isyscore"}"
: "${MYSQL_PASSWORD:="Isysc0re"}"
: "${MYSQL_DB:="Airflow"}"
: "${AIRFLOW_SQLCONN:="mysql://$MYSQL_USER:$MYSQL_PASSWORD@$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB?charset=utf8"}"

export \
  MYSQL_HOST \
  MYSQL_PORT \
  MYSQL_USER \
  MYSQL_PASSWORD \
  MYSQL_DB \
  AIRFLOW_SQLCONN \


case "$1" in
  initdb)
    #####
    ## 创建数据库设置参数
    ## cat /script/create_db_env.sql | sed 's/airflow_metadb/'$MYSQL_DB'/g' | mysql -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD
    sed -i 's/airflow_metadb/'$MYSQL_DB'/g' /script/create_db_env.sql
    case "$2" in
        -a)
        python /script/init_db.py
        ;;
    esac
    #####
    sleep 5
    airflow initdb
    sleep 10
    python /script/init_plugin_table.py
    sleep 5
    python /script/add_login_user.py
    ;;
  webserver)
    #####
    ## 判断是否需要初始化
    if [ $(mysql -N -s -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e \
        "select count(*) from information_schema.tables where \
            table_schema='Airflow' and table_name='dcmp_user_profile';") -eq 1 ]; then
        echo "table exist! start run server..."
    else
        echo "tables not exist, start initdb..."
        airflow initdb
        sleep 10
        python /script/init_plugin_table.py
        sleep 5
        python /script/add_login_user.py
        sleep 10
        echo "initdb over..."
    fi
    echo "airflow server over..."
    exec airflow scheduler &
    exec airflow webserver
    echo "airflow server running..."
    ;;
  worker|scheduler)
    # To give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
