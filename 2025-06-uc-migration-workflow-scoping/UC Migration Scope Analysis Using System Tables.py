# Databricks notebook source
# MAGIC %md
# MAGIC # UC Migration - Workflows Scope Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC The following is an analysis of the number of workflows that require to be migrated using system tables. This information should be combined with other sources such as UCX assessment results as well as SME meetings to account for deprecated workflows etc to come up with a final migration target.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List all jobs in workspace
# MAGIC
# MAGIC Filter Names as follows:
# MAGIC name NOT ILIKE '%views%' and name NOT ILIKE '%test%' and name NOT LIKE '%New %' and name NOT ILIKE '%Onetime%' and name NOT ILIKE '%UCX%'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT WorkspaceID DEFAULT '';

# COMMAND ----------

# MAGIC %md
# MAGIC This is a list of all the jobs in the system table - keep in mind that system tables have a default retention period of 1 year. If jobs older than that should be considered for migration, use the UCX assessment tool results to start from as a baseline.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT 
# MAGIC     job_id AS job_id,
# MAGIC     workspace_id,
# MAGIC     name,
# MAGIC     creator_id
# MAGIC FROM  
# MAGIC     system.lakeflow.jobs 
# MAGIC WHERE 
# MAGIC     workspace_id = ${WorkspaceID} 
# MAGIC     AND name != 'Untitled' 
# MAGIC     AND delete_time IS NULL 
# MAGIC     AND name NOT ILIKE '%views%' 
# MAGIC     AND name NOT ILIKE '%test%' 
# MAGIC     AND name NOT LIKE '%New %' 
# MAGIC     AND name NOT ILIKE '%Onetime%' 
# MAGIC     AND name NOT ILIKE '%UCX%'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Add Created Date
# MAGIC
# MAGIC We would like to infer the Created Date to use as one of the factors in determining if a workflow should be included in the migration.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW list_of_jobs AS (
# MAGIC     WITH created_date AS (
# MAGIC         SELECT DISTINCT 
# MAGIC             job_id AS job_id,
# MAGIC             workspace_id,
# MAGIC             name,
# MAGIC             change_time,
# MAGIC             FIRST_VALUE(change_time) OVER (PARTITION BY job_id ORDER BY change_time ASC) AS created_date,
# MAGIC             FIRST_VALUE(name) OVER (PARTITION BY job_id ORDER BY change_time DESC) AS latest_name,
# MAGIC             creator_id
# MAGIC         FROM  
# MAGIC             system.lakeflow.jobs 
# MAGIC         WHERE 
# MAGIC             workspace_id = ${WorkspaceID} 
# MAGIC             AND name != 'Untitled'
# MAGIC             AND delete_time IS NULL
# MAGIC             AND name NOT ILIKE '%views%'
# MAGIC             AND name NOT ILIKE '%test%'
# MAGIC             AND name NOT LIKE '%New %'
# MAGIC             AND name NOT ILIKE '%Onetime%'
# MAGIC             AND name NOT ILIKE '%UCX%'
# MAGIC     )
# MAGIC     SELECT DISTINCT 
# MAGIC         job_id,
# MAGIC         created_date,
# MAGIC         workspace_id,
# MAGIC         latest_name,
# MAGIC         creator_id
# MAGIC     FROM
# MAGIC         created_date
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Count of Job Runs
# MAGIC
# MAGIC Create a group by table from system.lakeflow.job_run_timeline to count how many times a job has been run over a period of time. This is another indicator of whether a workflow is relevant for migration or not. We would like to measure it through every quarter as well, to get an idea of frequency of runs through each quarter.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW count_of_runs AS (
# MAGIC
# MAGIC WITH total_runs AS (
# MAGIC   SELECT job_id, COUNT(*) AS Total_Runs, min(period_start_time) as earliest_execution_time , max(period_end_time) as latest_execution_time
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE 
# MAGIC   result_state = 'SUCCEEDED'
# MAGIC   AND workspace_id = ${WorkspaceID} 
# MAGIC   AND (run_type = 'JOB_RUN' OR run_type is null)
# MAGIC   GROUP BY job_id
# MAGIC ), 
# MAGIC
# MAGIC q1_runs AS (
# MAGIC   SELECT jrt.job_id, COUNT(*) AS q1_2024_Runs, SUM(t1.usage_quantity * list_prices.pricing.default) AS q1_2024_list_cost
# MAGIC   FROM system.lakeflow.job_run_timeline jrt
# MAGIC   INNER JOIN system.billing.usage t1
# MAGIC   ON t1.usage_metadata.job_id = jrt.job_id IS NOT NULL AND
# MAGIC   t1.usage_metadata.job_run_id = jrt.run_id 
# MAGIC   INNER JOIN system.billing.list_prices list_prices
# MAGIC     ON
# MAGIC       t1.cloud = list_prices.cloud AND
# MAGIC       t1.sku_name = list_prices.sku_name AND
# MAGIC       t1.usage_start_time >= list_prices.price_start_time AND
# MAGIC       (t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is NULL)
# MAGIC
# MAGIC   WHERE result_state = 'SUCCEEDED'
# MAGIC     AND jrt.period_start_time >= '2024-01-01'
# MAGIC     AND jrt.period_start_time < '2024-04-01'
# MAGIC     AND jrt.workspace_id = ${WorkspaceID} 
# MAGIC     AND (jrt.run_type = 'JOB_RUN' OR jrt.run_type is null)
# MAGIC       -- t1.workspace=
# MAGIC     AND t1.workspace_id = ${WorkspaceID}  
# MAGIC     AND t1.sku_name LIKE '%JOBS%'
# MAGIC   GROUP BY job_id
# MAGIC ),
# MAGIC
# MAGIC q2_runs AS (
# MAGIC   SELECT jrt.job_id, COUNT(*) AS q2_2024_Runs, SUM(t1.usage_quantity * list_prices.pricing.default) AS q2_2024_list_cost
# MAGIC   FROM system.lakeflow.job_run_timeline jrt
# MAGIC   INNER JOIN system.billing.usage t1
# MAGIC   ON t1.usage_metadata.job_id = jrt.job_id IS NOT NULL AND
# MAGIC   t1.usage_metadata.job_run_id = jrt.run_id 
# MAGIC   INNER JOIN system.billing.list_prices list_prices
# MAGIC     ON
# MAGIC       t1.cloud = list_prices.cloud AND
# MAGIC       t1.sku_name = list_prices.sku_name AND
# MAGIC       t1.usage_start_time >= list_prices.price_start_time AND
# MAGIC       (t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is NULL)
# MAGIC
# MAGIC   WHERE result_state = 'SUCCEEDED'
# MAGIC     AND jrt.period_start_time >= '2024-04-01'
# MAGIC     AND jrt.period_start_time < '2024-07-01'
# MAGIC     AND jrt.workspace_id = ${WorkspaceID} 
# MAGIC     AND (jrt.run_type = 'JOB_RUN' OR jrt.run_type is null)
# MAGIC       -- t1.workspace=
# MAGIC     AND t1.workspace_id = ${WorkspaceID}  
# MAGIC     AND t1.sku_name LIKE '%JOBS%'
# MAGIC   GROUP BY job_id
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC q3_runs AS (
# MAGIC   SELECT jrt.job_id, COUNT(*) AS q3_2024_Runs, SUM(t1.usage_quantity * list_prices.pricing.default) AS q3_2024_list_cost
# MAGIC   FROM system.lakeflow.job_run_timeline jrt
# MAGIC   INNER JOIN system.billing.usage t1
# MAGIC   ON t1.usage_metadata.job_id = jrt.job_id IS NOT NULL AND
# MAGIC   t1.usage_metadata.job_run_id = jrt.run_id 
# MAGIC   INNER JOIN system.billing.list_prices list_prices
# MAGIC     ON
# MAGIC       t1.cloud = list_prices.cloud AND
# MAGIC       t1.sku_name = list_prices.sku_name AND
# MAGIC       t1.usage_start_time >= list_prices.price_start_time AND
# MAGIC       (t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is NULL)
# MAGIC
# MAGIC   WHERE result_state = 'SUCCEEDED'
# MAGIC     AND jrt.period_start_time >= '2024-07-01'
# MAGIC     AND jrt.period_start_time < '2024-10-01'
# MAGIC     AND jrt.workspace_id = ${WorkspaceID} 
# MAGIC     AND (jrt.run_type = 'JOB_RUN' OR jrt.run_type is null)
# MAGIC       -- t1.workspace=
# MAGIC     AND t1.workspace_id = ${WorkspaceID}  
# MAGIC     AND t1.sku_name LIKE '%JOBS%'
# MAGIC   GROUP BY job_id
# MAGIC ),
# MAGIC
# MAGIC q4_runs AS (
# MAGIC   SELECT jrt.job_id, COUNT(*) AS q4_2024_Runs, SUM(t1.usage_quantity * list_prices.pricing.default) AS q4_2024_list_cost
# MAGIC   FROM system.lakeflow.job_run_timeline jrt
# MAGIC   INNER JOIN system.billing.usage t1
# MAGIC   ON t1.usage_metadata.job_id = jrt.job_id IS NOT NULL AND
# MAGIC   t1.usage_metadata.job_run_id = jrt.run_id 
# MAGIC   INNER JOIN system.billing.list_prices list_prices
# MAGIC     ON
# MAGIC       t1.cloud = list_prices.cloud AND
# MAGIC       t1.sku_name = list_prices.sku_name AND
# MAGIC       t1.usage_start_time >= list_prices.price_start_time AND
# MAGIC       (t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is NULL)
# MAGIC
# MAGIC   WHERE result_state = 'SUCCEEDED'
# MAGIC     AND jrt.period_start_time >= '2024-10-01'
# MAGIC     AND jrt.period_start_time <= '2024-12-31'
# MAGIC     AND jrt.workspace_id = ${WorkspaceID} 
# MAGIC     AND (jrt.run_type = 'JOB_RUN' OR jrt.run_type is null)
# MAGIC       -- t1.workspace=
# MAGIC     AND t1.workspace_id = ${WorkspaceID}  
# MAGIC     AND t1.sku_name LIKE '%JOBS%'
# MAGIC   GROUP BY job_id
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC q1_2025_runs AS (
# MAGIC   SELECT jrt.job_id, COUNT(*) AS q1_2025_Runs, SUM(t1.usage_quantity * list_prices.pricing.default) AS q1_2025_list_cost
# MAGIC   FROM system.lakeflow.job_run_timeline jrt
# MAGIC   INNER JOIN system.billing.usage t1
# MAGIC   ON t1.usage_metadata.job_id = jrt.job_id IS NOT NULL AND
# MAGIC   t1.usage_metadata.job_run_id = jrt.run_id 
# MAGIC   INNER JOIN system.billing.list_prices list_prices
# MAGIC     ON
# MAGIC       t1.cloud = list_prices.cloud AND
# MAGIC       t1.sku_name = list_prices.sku_name AND
# MAGIC       t1.usage_start_time >= list_prices.price_start_time AND
# MAGIC       (t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is NULL)
# MAGIC
# MAGIC   WHERE result_state = 'SUCCEEDED'
# MAGIC     AND jrt.period_start_time >= '2025-01-01'
# MAGIC     AND jrt.period_start_time < '2025-04-01'
# MAGIC     AND jrt.workspace_id = ${WorkspaceID} 
# MAGIC     AND (jrt.run_type = 'JOB_RUN' OR jrt.run_type is null)
# MAGIC       -- t1.workspace=
# MAGIC     AND t1.workspace_id = ${WorkspaceID}  
# MAGIC     AND t1.sku_name LIKE '%JOBS%'
# MAGIC   GROUP BY job_id
# MAGIC ),
# MAGIC
# MAGIC q2_2025_runs AS (
# MAGIC   SELECT jrt.job_id, COUNT(*) AS q2_2025_Runs, SUM(t1.usage_quantity * list_prices.pricing.default) AS q2_2025_list_cost
# MAGIC   FROM system.lakeflow.job_run_timeline jrt
# MAGIC   INNER JOIN system.billing.usage t1
# MAGIC   ON t1.usage_metadata.job_id = jrt.job_id IS NOT NULL AND
# MAGIC   t1.usage_metadata.job_run_id = jrt.run_id 
# MAGIC   INNER JOIN system.billing.list_prices list_prices
# MAGIC     ON
# MAGIC       t1.cloud = list_prices.cloud AND
# MAGIC       t1.sku_name = list_prices.sku_name AND
# MAGIC       t1.usage_start_time >= list_prices.price_start_time AND
# MAGIC       (t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is NULL)
# MAGIC
# MAGIC   WHERE result_state = 'SUCCEEDED'
# MAGIC     AND jrt.period_start_time >= '2025-04-01'
# MAGIC     AND jrt.period_start_time < '2025-07-01'
# MAGIC     AND jrt.workspace_id = ${WorkspaceID} 
# MAGIC     AND (jrt.run_type = 'JOB_RUN' OR jrt.run_type is null)
# MAGIC       -- t1.workspace=
# MAGIC     AND t1.workspace_id = ${WorkspaceID}  
# MAGIC     AND t1.sku_name LIKE '%JOBS%'
# MAGIC   GROUP BY job_id
# MAGIC ),
# MAGIC
# MAGIC all_executions AS (
# MAGIC
# MAGIC SELECT t.job_id, Total_Runs, q1_2024_Runs, q1_2024_list_cost, q2_2024_Runs, q2_2024_list_cost, q3_2024_Runs, q3_2024_list_cost, q4_2024_Runs, q4_2024_list_cost, q1_2025_Runs, q1_2025_list_cost, q2_2025_Runs, q2_2025_list_cost, earliest_execution_time, latest_execution_time
# MAGIC FROM total_runs t
# MAGIC LEFT JOIN q1_runs ON t.job_id = q1_runs.job_id
# MAGIC LEFT JOIN q2_runs ON t.job_id = q2_runs.job_id
# MAGIC LEFT JOIN q3_runs ON t.job_id = q3_runs.job_id
# MAGIC LEFT JOIN q4_runs ON t.job_id = q4_runs.job_id
# MAGIC LEFT JOIN q1_2025_runs ON t.job_id = q1_2025_runs.job_id
# MAGIC LEFT JOIN q2_2025_runs ON t.job_id = q2_2025_runs.job_id
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY latest_execution_time DESC) AS rn
# MAGIC     FROM all_executions
# MAGIC ) t
# MAGIC WHERE t.rn = 1
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cost of Job Runs
# MAGIC
# MAGIC Here, we aggregate the cost of a job over multiple runs. These are list prices, but they should give a comparative overview of which workloads are most expensive, and they can also serve as a prioritization metric for workflow scope.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW job_cost_analysis AS (
# MAGIC SELECT
# MAGIC   t1.usage_metadata.job_id as job_id,
# MAGIC   SUM(t1.usage_quantity * list_prices.pricing.default) AS list_cost
# MAGIC FROM system.billing.usage t1
# MAGIC   INNER JOIN system.billing.list_prices list_prices
# MAGIC     ON
# MAGIC       t1.cloud = list_prices.cloud AND
# MAGIC       t1.sku_name = list_prices.sku_name AND
# MAGIC       t1.usage_start_time >= list_prices.price_start_time AND
# MAGIC       (t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is NULL)
# MAGIC WHERE
# MAGIC   -- t1.workspace=
# MAGIC   t1.workspace_id = ${WorkspaceID}  AND
# MAGIC   t1.sku_name LIKE '%JOBS%' AND
# MAGIC   t1.usage_metadata.job_id IS NOT NULL AND
# MAGIC   t1.usage_metadata.job_run_id IS NOT NULL
# MAGIC GROUP BY job_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Accounting for Job Run Types
# MAGIC
# MAGIC JOB_RUN	Standard job execution from	Jobs & Job Runs UI	/jobs and /jobs/runs endpoints	jobs, job_tasks, job_run_timeline, job_task_run_timeline
# MAGIC
# MAGIC SUBMIT_RUN	One-time run via POST /jobs/runs/submit	and we can exclude this from the scope.
# MAGIC
# MAGIC WORKFLOW_RUN	Run initiated from notebook workflow and these are not in the UI	Not accessible	job_run_timeline
# MAGIC
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/jobs#run-type

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Type_Job_Run AS (
# MAGIC   SELECT 
# MAGIC     DISTINCT jrt.job_id
# MAGIC   FROM 
# MAGIC     system.lakeflow.job_run_timeline AS jrt
# MAGIC   WHERE 
# MAGIC     jrt.period_start_time > '2024-01-01'
# MAGIC     AND (jrt.run_type = 'JOB_RUN' OR jrt.run_type IS NULL)
# MAGIC     AND jrt.workspace_id = ${WorkspaceID} 
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Job runs that are running in UC Shared Clusters, indicating that the jobs are already in UC
# MAGIC
# MAGIC
# MAGIC Sometimes, in many large enterprises, you may have situations where few jobs are already running on UC Shared Clusters and do not require a migration effort. We can identify those and exclude them from the overall scope by identifying if they are using UC Shared Clusters. UC Shared Clusters are the preferred cluster to migrate to, when going through a UC Migration. There a few assumptions here:
# MAGIC
# MAGIC 1. We are assuming even if 1 task in a job is using Shared Cluster, the whole workflow is UC compliant. Verify for your situation, if this assumption is valid, and if not, tweak the query accordingly.
# MAGIC 2. A workflow that runs on Single User cluster may also be compliant with UC, but we are not considering it here, as that would mask other underlying tech debt such as use of RDD in the workflows etc. Hence, for the purpose of scoping, strictly only jobs running on shared clusters should be counted in scope, while single user cluster may be left as is and considered UC compliant on a case by case basis.
# MAGIC
# MAGIC
# MAGIC https://docs.databricks.com/api/workspace/clusters/get#data_security_mode

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW uc_job_run_compute AS (
# MAGIC WITH job_run_compute AS (
# MAGIC     SELECT 
# MAGIC         DISTINCT jtrt.job_id, 
# MAGIC         COALESCE(jrt.run_type, 'UNKNOWN') AS job_run_type,
# MAGIC         TRY_ELEMENT_AT(jtrt.compute_ids, 1) AS compute_id
# MAGIC     FROM 
# MAGIC         system.lakeflow.job_task_run_timeline AS jtrt
# MAGIC     INNER JOIN 
# MAGIC         system.lakeflow.job_run_timeline AS jrt
# MAGIC     ON 
# MAGIC         jtrt.job_id = jrt.job_id
# MAGIC         AND jtrt.job_run_id = jrt.run_id
# MAGIC     WHERE 
# MAGIC         jrt.result_state = 'SUCCEEDED'
# MAGIC         AND jrt.workspace_id = ${WorkspaceID} 
# MAGIC         AND jtrt.workspace_id = ${WorkspaceID} 
# MAGIC ),
# MAGIC
# MAGIC cluster_id_to_name AS (
# MAGIC     SELECT 
# MAGIC         request_params.cluster_name AS cluster_name, 
# MAGIC         GET_JSON_OBJECT(response.result, '$.cluster_id') AS cluster_id 
# MAGIC     FROM 
# MAGIC         system.access.audit
# MAGIC     WHERE
# MAGIC         service_name = 'clusters'
# MAGIC         AND action_name IN ('create', 'edit')
# MAGIC         AND request_params.data_security_mode IN ('USER_ISOLATION')
# MAGIC         AND workspace_id = ${WorkspaceID} 
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     DISTINCT jrc.job_id, 
# MAGIC     jrc.job_run_type, 
# MAGIC     jrc.compute_id, 
# MAGIC     cin.cluster_id, 
# MAGIC     cin.cluster_name
# MAGIC FROM 
# MAGIC     job_run_compute AS jrc
# MAGIC INNER JOIN
# MAGIC     cluster_id_to_name AS cin
# MAGIC ON 
# MAGIC     cin.cluster_id = jrc.compute_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Add User Data 

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from pyspark.sql.types import StructType, StructField, StringType

w = WorkspaceClient()

# COMMAND ----------

all_users = w.users.list(
    attributes="id,userName, display_name, emails",
    sort_by="userName",
    sort_order=iam.ListSortOrder.DESCENDING,
)

user_email_list = [
    {"id": u.id, "user_name": u.user_name, "display_name": u.display_name, "email": u.emails[0].value if u.emails else None} 
    for u in all_users
]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("display_name", StringType(), True),
    StructField("email", StringType(), True)
])

all_users_df = spark.createDataFrame(user_email_list, schema)
all_users_df.createOrReplaceTempView("all_users_view")
display(all_users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Bringing it all together

# COMMAND ----------

# MAGIC %md
# MAGIC To bring it all together, we join the overall list of jobs with the following:
# MAGIC - List of job run types that are in scope i.e. JOB_RUN type etc.
# MAGIC - Considering workflows that are already in UC i.e. using Shared Clusters (Anti Join as we want to exclude these)
# MAGIC - Include metrics on number of types they are run as well as their cost.
# MAGIC
# MAGIC
# MAGIC Use this as a starting point for discussion with various stakeholders for further rationalization of the scope of a UC migration.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     lj.*, 
# MAGIC     jca.*, 
# MAGIC     cor.*,
# MAGIC     auv.user_name
# MAGIC
# MAGIC FROM 
# MAGIC     list_of_jobs lj
# MAGIC INNER JOIN 
# MAGIC     Type_Job_Run tjr
# MAGIC     ON lj.job_id = tjr.job_id
# MAGIC LEFT JOIN 
# MAGIC     job_cost_analysis jca
# MAGIC     ON lj.job_id = jca.job_id
# MAGIC LEFT JOIN 
# MAGIC     count_of_runs cor
# MAGIC     ON lj.job_id = cor.job_id
# MAGIC LEFT ANTI JOIN 
# MAGIC     uc_job_run_compute ujrc
# MAGIC     ON lj.job_id = ujrc.job_id
# MAGIC LEFT JOIN
# MAGIC     all_users_view auv
# MAGIC     ON lj.creator_id = auv.id
# MAGIC ORDER BY 
# MAGIC     jca.list_cost DESC,  
# MAGIC     cor.Total_Runs DESC, 
# MAGIC     cor.latest_execution_time DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS main.uc_workflow_scoping

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.uc_workflow_scoping.sample AS 
# MAGIC SELECT 
# MAGIC     lj.*, 
# MAGIC     jca.list_cost, 
# MAGIC     cor.Total_Runs, 
# MAGIC     cor.q1_2024_Runs, 
# MAGIC     cor.q1_2024_list_cost, 
# MAGIC     cor.q2_2024_Runs, 
# MAGIC     cor.q2_2024_list_cost, 
# MAGIC     cor.q3_2024_Runs, 
# MAGIC     cor.q3_2024_list_cost, 
# MAGIC     cor.q4_2024_Runs, 
# MAGIC     cor.q4_2024_list_cost, 
# MAGIC     cor.q1_2025_Runs, 
# MAGIC     cor.q1_2025_list_cost, 
# MAGIC     cor.q2_2025_Runs, 
# MAGIC     cor.q2_2025_list_cost,
# MAGIC     cor.earliest_execution_time, 
# MAGIC     cor.latest_execution_time,
# MAGIC     auv.user_name
# MAGIC FROM 
# MAGIC     list_of_jobs lj
# MAGIC INNER JOIN 
# MAGIC     Type_Job_Run tjr
# MAGIC     ON lj.job_id = tjr.job_id
# MAGIC LEFT JOIN 
# MAGIC     job_cost_analysis jca
# MAGIC     ON lj.job_id = jca.job_id
# MAGIC LEFT JOIN 
# MAGIC     count_of_runs cor
# MAGIC     ON lj.job_id = cor.job_id
# MAGIC LEFT ANTI JOIN 
# MAGIC     uc_job_run_compute ujrc
# MAGIC     ON lj.job_id = ujrc.job_id
# MAGIC LEFT JOIN
# MAGIC     all_users_view auv
# MAGIC     ON lj.creator_id = auv.id
# MAGIC ORDER BY 
# MAGIC     jca.list_cost DESC,  
# MAGIC     cor.Total_Runs DESC, 
# MAGIC     cor.latest_execution_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC
