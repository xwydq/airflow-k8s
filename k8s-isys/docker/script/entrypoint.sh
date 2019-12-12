#!/bin/bash

chmod +x $AIRFLOW_HOME/dags/tools/dagx_render.sh

python /script/init_plugin_table.py
sleep 5
python /script/add_login_user.py

case "$1" in
  webserver)
    echo "airflow webserver server over..."
    exec airflow webserver
    echo "airflow webserver server running..."
    ;;
  scheduler)
    echo "airflow scheduler server over..."
    exec airflow scheduler
    echo "airflow scheduler server running..."
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
