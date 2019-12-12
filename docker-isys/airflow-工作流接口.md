### airflow工作流接口说明

#### 状态分类

DAG工作流状态分类： `running-运行中；failed-失败；success-成功`

TASK作业状态分类：`scheduled-已排程；queued-入队；skipped-跳过；failed-失败； running-运行中；success-成功  `

测试url：`192.168.9.204:8080`

#### 按日获取任务流运行状态信息

请求地址：`/api/experimental/dags_daysum`

请求参数：查询日期`execution_date`（格式：`2019-09-22`）

示例：` http://<url>/api/experimental/dags_daysum?execution_date=2019-10-23 `






#### 按周获取任务流运行状态信息

请求地址：`/api/experimental/dags_weeksum`

请求参数：查询日期`execution_date`（格式：`2019-09-22`）

结果：获取请求日期前一周的数据

示例：` http://<url>/api/experimental/dags_weeksum?execution_date=2019-10-23 `




#### 任务流详情表格

请求地址：`/api/experimental/dags_table`

请求参数：日期范围`start_date`; `end_date`（格式：`2019-09-22`）；page；size

结果：获取请求日期范围内的任务流数据

示例：` http://<url>/api/experimental/dags_table?start_date=2019-10-23&end_date=2019-10-23&page=1&size=20  `




#### 按日获取task 作业运行状态信息

请求地址：`/api/experimental/tasks_daysum`

请求参数：查询日期`execution_date`（格式：`2019-09-22`）

示例：` http://<url>/api/experimental/tasks_daysum?execution_date=2019-10-23  `





#### 按周获取task 作业运行状态信息

请求地址：`/api/experimental/tasks_weeksum`

请求参数：查询日期`execution_date`（格式：`2019-09-22`）

结果：获取请求日期近一周的作业数据

示例：` http://<url>/api/experimental/tasks_weeksum?execution_date=2019-10-23 `




