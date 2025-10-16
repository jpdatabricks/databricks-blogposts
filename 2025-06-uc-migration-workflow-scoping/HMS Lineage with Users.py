# Databricks notebook source
# MAGIC %md
# MAGIC # Using HMS Lineage to extract user names, job owner name, query names etc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instantiate Workspace Client from Databricks SDK

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from pyspark.sql.types import StructType, StructField, StringType

w = WorkspaceClient()



# COMMAND ----------

# MAGIC %md
# MAGIC ## Get list of users and their email addresses

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

display(all_users_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get list of jobs and their creator user names

# COMMAND ----------

all_jobs = w.jobs.list(expand_tasks=False)

display(all_jobs)

# COMMAND ----------

from pyspark.sql import Row

job_list = [
    {"job_id": u.job_id, "creator_user_name": u.creator_user_name} 
    for u in all_jobs
]

job_schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("creator_user_name", StringType(), True)
])

all_jobs_df = spark.createDataFrame(job_list, job_schema)

display(all_jobs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get list of queries as well as their owner name, and catalog and schema they refer to

# COMMAND ----------

all_queries = w.queries.list()

display(all_queries)

# COMMAND ----------

from pyspark.sql import Row

query_list = [
    {"query_id": u.id, 
     "query_name": u.display_name,
     "owner_user_name": u.owner_user_name,
     "last_modifier_user_name": u.last_modifier_user_name,
     "catalog": u.catalog,
     "schema": u.schema,     
     } 
    for u in all_queries
]





# COMMAND ----------

query_schema = StructType([
    StructField("query_id", StringType(), True),
    StructField("query_name", StringType(), True),
    StructField("owner_user_name", StringType(), True),
    StructField("last_modifier_user_name", StringType(), True),
    StructField("catalog", StringType(), True),
    StructField("schema", StringType(), True)
])

# COMMAND ----------

all_queries_df = spark.createDataFrame(query_list, query_schema)

display(all_queries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get list of dashboards as well as their names

# COMMAND ----------

all_dashboards = w.dashboards.list()

display(all_dashboards) 

# COMMAND ----------

from pyspark.sql import Row

dashboard_list = [
    {"dashboard_id": u.id, 
     "dashboard_name": u.name,  
     } 
    for u in all_dashboards
]

dashboard_schema = StructType([
    StructField("dashboard_id", StringType(), True),
    StructField("dashboard_name", StringType(), True)
])

all_dashboards_df = spark.createDataFrame(dashboard_list, dashboard_schema)

display(all_dashboards_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from HMS to UC migration System Table and Join above extracted details.

# COMMAND ----------

# 2. Read your table and join it with the user data
table_access_df = spark.read.table("system.hms_to_uc_migration.table_access")

# Use a broadcast hint if the users_df is small enough to fit in memory on each executor
result_df = table_access_df.join(all_users_df.hint("broadcast"), table_access_df.created_by == all_users_df.id, "left") \
                           .join(all_jobs_df, table_access_df.entity_id == all_jobs_df.job_id, "left") \
                           .join(all_queries_df, table_access_df.entity_id == all_queries_df.query_id, "left") \
                           .join(all_dashboards_df, table_access_df.entity_id == all_dashboards_df.dashboard_id, "left")

display(result_df.where(result_df.created_by.isNotNull()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist results to a Delta Table. 
# MAGIC From here - we can further take actions by email users about their artifacts that need migration to UC.

# COMMAND ----------

result_df.write.mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("prashanth_subrahmanyam_catalog.uc_workflow_scoping.hms_to_uc_lineage")
