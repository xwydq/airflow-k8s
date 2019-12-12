docker 部署 airflow 步骤



### 下载 Dockerfile及安装包

```
git clone ssh://git@10.30.30.3:10022/bigdatagroup/docker-airflow.git
```



### MySQL数据库配置

```
vi /etc/my.cnf
[mysqld]
explicit_defaults_for_timestamp = 1

# mysql restart
systemctl restart mysqld

# mac brew mysql restart
brew services restart mysql@5.7
```



### 修改MySQL数据库配置信息

```
vi script/entrypoint.sh

: "${MYSQL_HOST:="192.168.10.143"}"
: "${MYSQL_PORT:="3306"}"
: "${MYSQL_USER:="root"}"
: "${MYSQL_PASSWORD:="jc1116"}"
: "${MYSQL_DB:="airflow_demo"}"
```



### build

```shell
cd docker-airflow-all
#docker image rm -f zlj-dockerairflow
docker build -t zlj-dockerairflow .
```



### 数据表及插件所需表初始化
#### 初始化方案一：
以下命令会按照`script/entrypoint.sh` 设置的`MySQL`信息创建`airflow`所需元数据表及插件所用表，如果存在指定元数据库会覆盖（需要对应权限）。如果已经独立完成相关表的创建可以跳过该步

```
# 数据库已创建
docker run --rm -ti -p 8080:8080 -v $(pwd)/dags/dagxdeployed/:/home/airflow/airflow/dags/dagxdeployed -v $(pwd)/logs/:/home/airflow/airflow/logs -v $(pwd)/files/:/home/airflow/airflow/files zlj-dockerairflow initdb
# 数据库未创建（需要建库权限）
docker run --rm -ti -p 8080:8080 -v $(pwd)/dags/dagxdeployed/:/home/airflow/airflow/dags/dagxdeployed -v $(pwd)/logs/:/home/airflow/airflow/logs -v $(pwd)/files/:/home/airflow/airflow/files zlj-dockerairflow initdb -a
```

#### 初始化方案二：
`script/entrypoint.sh` 设置好`MySQL`信息后直接启动`airflow`服务，在启动之前会判断是否需要初始化数据库


### run
```
#docker container rm -f zlj_airflow
docker run --name zlj_airflow -d -p 8080:8080 -e LOAD_EX=y -v $(pwd)/dags/dagxdeployed:/home/airflow/airflow/dags/dagxdeployed -v $(pwd)/logs/:/home/airflow/airflow/logs -v $(pwd)/files/:/home/airflow/airflow/files zlj-dockerairflow webserver
```

### 备注
挂载：将当前路径下的 `dags`, `logs`, `files` 分别挂载到 `/home/airflow/airflow`下，具体对应关系：
1. `-v $(pwd)/dags/dagxdeployed:/home/airflow/airflow/dags/dagxdeployed`
2. `-v $(pwd)/logs/:/home/airflow/airflow/logs`
3. `-v $(pwd)/files/:/home/airflow/airflow/files`

**确保`dags`下子目录及文件存在**

`dags/tools`目录为系统默认的dag文件，不新增文件。`dags`目录下会新增