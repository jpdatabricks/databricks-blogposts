# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Setup for Multi-Country Advisory
# MAGIC
# MAGIC This notebook sets up everything needed for the retirement advisory agent:
# MAGIC catalog, schemas, tables, data, and UC functions.
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Creates catalog and schemas (configured via config.yaml)
# MAGIC - Creates tables (member_profiles, citation_registry, governance)
# MAGIC - Loads citation registry data (regulatory references)
# MAGIC - Creates Unity Catalog functions for retirement calculations
# MAGIC - Generates and loads member profiles (29 members across AU, US, UK, IN)
# MAGIC - Tests function execution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install -q -r ../requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import sys
import os

repo_root = os.path.abspath(
    os.path.join(os.getcwd(), "..")
)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from src.config import UNITY_CATALOG, UNITY_SCHEMA, FUNCTIONS_SCHEMA

catalog = UNITY_CATALOG
schema = UNITY_SCHEMA
functions_schema = FUNCTIONS_SCHEMA

print(f"Setting up UC resources in:")
print(f"  Catalog: {catalog}")
print(f"  Data Schema: {schema}")
print(f"  Functions Schema: {functions_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema
# MAGIC
# MAGIC First, ensure the catalog and schema exist before creating functions

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
print(f"✓ Catalog '{catalog}' is ready")

# Use the catalog
spark.sql(f"USE CATALOG {catalog}")
print(f"✓ Using catalog '{catalog}'")

# Create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {functions_schema}")
print(f"✓ Schemas created: {schema}, {functions_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables
# MAGIC
# MAGIC Create the required tables: citation_registry, governance, member_profiles

# COMMAND ----------

# Create citation_registry table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.citation_registry (
  citation_id STRING COMMENT 'Unique citation identifier (e.g., AU-TAX-001)',
  country STRING COMMENT 'Country code (AU, US, UK, IN)',
  authority STRING COMMENT 'Regulatory authority name',
  regulation_name STRING COMMENT 'Name of regulation or standard',
  regulation_code STRING COMMENT 'Specific code or section reference',
  effective_date DATE COMMENT 'Date regulation became effective',
  source_url STRING COMMENT 'Official URL to regulation',
  description STRING COMMENT 'Brief description of what this regulates',
  last_verified TIMESTAMP COMMENT 'When citation was last verified',
  tool_type STRING COMMENT 'Tool type: tax, benefit, projection'
)
USING delta
COMMENT 'Registry of regulatory citations for compliance'
""")
print(f"✓ Created table: {catalog}.{schema}.citation_registry")

# COMMAND ----------

# Create governance table for audit logging
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.governance (
  event_id STRING COMMENT 'Unique event identifier',
  timestamp TIMESTAMP COMMENT 'Event timestamp',
  user_id STRING COMMENT 'Member ID who made query',
  session_id STRING COMMENT 'Session identifier',
  country STRING COMMENT 'Country context',
  query_string STRING COMMENT 'User query text',
  agent_response STRING COMMENT 'Agent generated response',
  result_preview STRING COMMENT 'Preview of results',
  cost DOUBLE COMMENT 'Estimated cost in USD',
  citations STRING COMMENT 'JSON array of citations used',
  tool_used STRING COMMENT 'Tool used for query execution',
  judge_response STRING COMMENT 'Validation response JSON',
  judge_verdict STRING COMMENT 'Quality assessment verdict (Pass/Fail)',
  judge_confidence DOUBLE COMMENT 'Validation confidence score (0.0-1.0)',
  error_info STRING COMMENT 'Error information if any',
  validation_mode STRING COMMENT 'Validation strategy used',
  validation_attempts BIGINT COMMENT 'Number of validation attempts',
  total_time_seconds DOUBLE COMMENT 'Total execution time'
)
USING delta
PARTITIONED BY (country)
COMMENT 'Audit log for all agent queries and tool executions'
""")
print(f"✓ Created table: {catalog}.{schema}.governance")

# COMMAND ----------

# Create member_profiles table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.member_profiles (
  member_id STRING COMMENT 'Unique member identifier',
  name STRING COMMENT 'Member full name',
  age INT COMMENT 'Member age',
  country STRING COMMENT 'Member country (AU, US, UK, IN)',
  super_balance DOUBLE COMMENT 'Current retirement account balance',
  marital_status STRING COMMENT 'Marital status (Single, Married, Divorced, Widowed)',
  other_assets DOUBLE COMMENT 'Other assessable assets',
  preservation_age INT COMMENT 'Preservation age for tax-free access',
  account_based_pension DOUBLE COMMENT 'Account-based pension amount if already drawing down',
  annual_income_outside_super DOUBLE COMMENT 'Annual income from sources outside retirement account',
  debt DOUBLE COMMENT 'Total outstanding debt',
  dependents INT COMMENT 'Number of dependents',
  employment_status STRING COMMENT 'Employment status (Full-time, Part-time, Retired, Self-employed, etc.)',
  financial_literacy STRING COMMENT 'Financial literacy level (Low, Medium, Moderate, High)',
  gender STRING COMMENT 'Gender (Male, Female)',
  health_status STRING COMMENT 'Health status (Excellent, Good, Fair, Poor, Chronic illness)',
  home_ownership STRING COMMENT 'Home ownership status (Owned Outright, Owned with Mortgage, Renting, etc.)',
  persona_type STRING COMMENT 'Member persona type for segmentation',
  risk_profile STRING COMMENT 'Investment risk profile (Conservative, Balanced, Moderate, Growth, Aggressive)'
)
USING delta
COMMENT 'Member profile information for retirement calculations'
""")
print(f"✓ Created table: {catalog}.{schema}.member_profiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions

# COMMAND ----------

# Grant permissions on governance table (needed for INSERT operations)
current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Granting permissions to current user: {current_user}")

try:
    # Grant to current user
    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog}.{schema}.governance TO `{current_user}`")
    print(f"✓ Granted SELECT, MODIFY on governance table to {current_user}")

    # Grant to all users (if you want broader access)
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.governance TO `account users`")
    print(f"✓ Granted SELECT on governance table to all users")

    # Grant on member_profiles (needed for queries)
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.member_profiles TO `{current_user}`")
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.member_profiles TO `account users`")
    print(f"✓ Granted SELECT on member_profiles table")

    # Grant on citation_registry (needed for citations)
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.citation_registry TO `{current_user}`")
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.citation_registry TO `account users`")
    print(f"✓ Granted SELECT on citation_registry table")

except Exception as e:
    print(f"⚠️ Permission grant warning: {e}")
    print(f"   You may need admin privileges to grant permissions")
    print(f"   Or permissions may already be inherited from schema/catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Initial Citation Data
# MAGIC
# MAGIC Load regulatory citations needed by UC functions

# COMMAND ----------

# Create temporary view with citation data for MERGE operation
citation_data = [
    ('AU-TAX-001', 'AU', 'Australian Taxation Office', 'Income Tax Assessment Act 1997', 'Division 301',
     '1997-07-01', 'https://www.legislation.gov.au/Series/C2004A04868',
     'Superannuation lump sum taxation rules', 'tax'),
    ('AU-PENSION-001', 'AU', 'Department of Social Services', 'Social Security Act 1991', 'Part 3.10',
     '1991-07-01', 'https://www.legislation.gov.au/Series/C2004A04770',
     'Age Pension asset test thresholds', 'benefit'),
    ('AU-STANDARD-001', 'AU', 'ASFA', 'ASFA Retirement Standard', 'Annual Report',
     '2024-01-01', 'https://www.superannuation.asn.au/retirement-standard',
     'Retirement income adequacy standards', 'projection'),
    ('US-TAX-001', 'US', 'Internal Revenue Service', 'Internal Revenue Code', 'Section 72(t)',
     '1986-10-01', 'https://www.irs.gov/retirement-plans/plan-participant-employee/retirement-topics-tax-on-early-distributions',
     '10% early withdrawal penalty before age 59.5', 'tax'),
    ('US-PENALTY-001', 'US', 'Internal Revenue Service', 'Internal Revenue Code', 'Section 72(t)',
     '1986-10-01', 'https://www.irs.gov/retirement-plans/plan-participant-employee/retirement-topics-exceptions-to-tax-on-early-distributions',
     'Exceptions to early withdrawal penalty', 'tax'),
    ('US-SS-001', 'US', 'Social Security Administration', 'Social Security Act', 'Title II',
     '1935-08-14', 'https://www.ssa.gov/benefits/retirement/planner/',
     'Social Security retirement benefits eligibility', 'benefit'),
    ('US-RMD-001', 'US', 'Internal Revenue Service', 'SECURE 2.0 Act', 'IRC Section 401(a)(9)',
     '2023-01-01', 'https://www.irs.gov/retirement-plans/plan-participant-employee/retirement-topics-required-minimum-distributions-rmds',
     'Required Minimum Distribution rules starting at age 73', 'projection'),
    ('UK-TAX-001', 'UK', 'HM Revenue & Customs', 'Finance Act 2004', 'Part 4',
     '2006-04-06', 'https://www.gov.uk/tax-on-your-private-pension',
     'Pension tax-free lump sum (25% up to £268,275)', 'tax'),
    ('UK-PENSION-001', 'UK', 'Department for Work and Pensions', 'Pensions Act 2014', 'Part 1',
     '2016-04-06', 'https://www.gov.uk/new-state-pension',
     'New State Pension eligibility and rates', 'benefit'),
    ('UK-DRAWDOWN-001', 'UK', 'Financial Conduct Authority', 'Pension Freedoms', 'COBS 19.5',
     '2015-04-06', 'https://www.fca.org.uk/publication/policy/ps15-04.pdf',
     'Pension drawdown flexibility rules', 'projection'),
    ('IN-EPF-001', 'IN', 'EPFO', 'Employees Provident Funds Act 1952', 'Section 10(12)',
     '1952-03-04', 'https://www.epfindia.gov.in/',
     'EPF contribution limits and tax exemptions', 'tax'),
    ('IN-TAX-001', 'IN', 'Income Tax Department', 'Income Tax Act 1961', 'Section 80C',
     '1961-04-01', 'https://www.incometax.gov.in/',
     'EPF withdrawal taxation rules', 'tax'),
    ('IN-NPS-001', 'IN', 'PFRDA', 'PFRDA Act 2013', 'Section 22',
     '2013-09-27', 'https://www.pfrda.org.in/',
     'NPS 40% annuity requirement and 60% tax-free lump sum', 'benefit'),
    ('IN-EPS-001', 'IN', 'EPFO', 'Employees Pension Scheme 1995', 'Regulation 11',
     '1995-11-16', 'https://www.epfindia.gov.in/site_docs/PDFs/Downloads_PDFs/EPS95Eng.pdf',
     'EPS pension calculation formula', 'eps_benefit'),
    ('IN-INTEREST-001', 'IN', 'EPFO', 'EPF Interest Rate Notification', 'Annual',
     '2024-04-01', 'https://www.epfindia.gov.in/',
     'EPF interest rate (currently 8.15% for 2023-24)', 'projection')
]

# Create temp view
from pyspark.sql.types import StructType, StructField, StringType
citation_schema = StructType([
    StructField("citation_id", StringType(), False),
    StructField("country", StringType(), False),
    StructField("authority", StringType(), False),
    StructField("regulation_name", StringType(), False),
    StructField("regulation_code", StringType(), True),
    StructField("effective_date", StringType(), False),
    StructField("source_url", StringType(), True),
    StructField("description", StringType(), True),
    StructField("tool_type", StringType(), False)
])

df_citations = spark.createDataFrame(citation_data, citation_schema)
df_citations.createOrReplaceTempView("new_citations")

# MERGE to update existing and insert new citations
spark.sql(f"""
MERGE INTO {catalog}.{schema}.citation_registry AS target
USING new_citations AS source
ON target.citation_id = source.citation_id
WHEN MATCHED THEN UPDATE SET
    target.country = source.country,
    target.authority = source.authority,
    target.regulation_name = source.regulation_name,
    target.regulation_code = source.regulation_code,
    target.effective_date = source.effective_date,
    target.source_url = source.source_url,
    target.description = source.description,
    target.last_verified = current_timestamp(),
    target.tool_type = source.tool_type
WHEN NOT MATCHED THEN INSERT (
    citation_id, country, authority, regulation_name, regulation_code,
    effective_date, source_url, description, last_verified, tool_type
) VALUES (
    source.citation_id, source.country, source.authority, source.regulation_name,
    source.regulation_code, source.effective_date, source.source_url,
    source.description, current_timestamp(), source.tool_type
)
""")

citation_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.citation_registry").collect()[0].cnt
print(f"✓ Merged citations - total records: {citation_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create UC Functions from SQL DDL
# MAGIC
# MAGIC Load and execute the UC functions SQL script

# COMMAND ----------

# Read the UC functions SQL file
import os
sql_file_path = "../sql_ddls/super_advisory_demo_functions.sql"

with open(sql_file_path, 'r') as f:
    sql_content = f.read()

# Replace hardcoded catalog and schema names with configured values
sql_content = sql_content.replace("pension_blog", catalog)
sql_content = sql_content.replace("pension_calculators", functions_schema)

print(f"✓ Loaded UC functions SQL from {sql_file_path}")
print(f"  Replaced 'pension_blog' with '{catalog}'")
print(f"  Replaced 'pension_calculators' with '{functions_schema}'")
print(f"  Size: {len(sql_content):,} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Function Creation
# MAGIC
# MAGIC Split and execute each CREATE FUNCTION statement

# COMMAND ----------

# Split into individual statements (separated by semicolons outside of function bodies)
# This is a simplified approach - for production, use proper SQL parser

statements = []
current_stmt = []
in_function = False

for line in sql_content.split('\n'):
    line_stripped = line.strip()

    if line_stripped.startswith('CREATE OR REPLACE FUNCTION'):
        in_function = True
        if current_stmt:
            statements.append('\n'.join(current_stmt))
            current_stmt = []

    current_stmt.append(line)

    if in_function and line_stripped == ';':
        statements.append('\n'.join(current_stmt))
        current_stmt = []
        in_function = False

if current_stmt:
    statements.append('\n'.join(current_stmt))

print(f"Found {len([s for s in statements if 'CREATE' in s])} CREATE FUNCTION statements")

# COMMAND ----------

# Execute each CREATE FUNCTION statement
success_count = 0
error_count = 0

for i, stmt in enumerate(statements):
    # Skip empty statements or comments-only
    stmt_stripped = stmt.strip()
    if not stmt_stripped or not 'CREATE OR REPLACE FUNCTION' in stmt:
        continue

    # Skip if it's all comments
    if all(line.strip().startswith('--') or not line.strip() for line in stmt.split('\n')):
        continue

    try:
        # Extract function name for logging
        func_name = stmt.split('FUNCTION')[1].split('(')[0].strip()

        spark.sql(stmt)
        print(f"✓ Created function: {func_name}")
        success_count += 1
    except Exception as e:
        print(f"✗ Error creating function: {str(e)[:100]}")
        error_count += 1

print(f"\nSummary:")
print(f"  Success: {success_count}")
print(f"  Errors: {error_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Created Functions
# MAGIC
# MAGIC Display only user-defined functions (not built-in Spark functions)

# COMMAND ----------

display(spark.sql(f"SHOW USER FUNCTIONS IN {catalog}.{functions_schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Function Execution
# MAGIC
# MAGIC Test key functions for each country

# COMMAND ----------

# MAGIC %md
# MAGIC ### Australia - Tax Calculation

# COMMAND ----------

result_au = spark.sql(f"""
SELECT {catalog}.{functions_schema}.au_calculate_tax(
    'M12345',  -- member_id
    67,        -- member_age
    60,        -- preservation_age
    500000.0,  -- super_balance
    50000.0    -- withdrawal_amount
) as tax_result
""")

display(result_au)

# COMMAND ----------

# MAGIC %md
# MAGIC ### United States - 401(k) Tax Calculation

# COMMAND ----------

result_us = spark.sql(f"""
SELECT {catalog}.{functions_schema}.us_calculate_401k_tax(
    'M67890',  -- member_id
    '401k',    -- account_type
    50000.0,   -- withdrawal_amount
    55         -- member_age (under 59.5, so early penalty applies)
) as tax_result
""")

display(result_us)

# COMMAND ----------

# MAGIC %md
# MAGIC ### United Kingdom - State Pension Check

# COMMAND ----------

result_uk = spark.sql(f"""
SELECT {catalog}.{functions_schema}.uk_check_state_pension(
    'M11223',    -- member_id
    67,          -- member_age
    35,          -- ni_qualifying_years
    'married'    -- marital_status
) as pension_result
""")

display(result_uk)

# COMMAND ----------

# MAGIC %md
# MAGIC ### India - EPF Tax Calculation

# COMMAND ----------

result_in = spark.sql(f"""
SELECT {catalog}.{functions_schema}.in_calculate_epf_tax(
    'M44556',   -- member_id
    55,         -- member_age
    500000.0,   -- super_balance (EPF balance)
    50000.0     -- withdrawal_amount
) as tax_result
""")

display(result_in)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Permissions
# MAGIC
# MAGIC **Default Permissions:**
# MAGIC - This grants EXECUTE permission to all `account users` group
# MAGIC - Functions can be called by any user in your Databricks account
# MAGIC
# MAGIC **Customize for your security requirements:**
# MAGIC - Change `account users` to a specific group (e.g., `data_scientists`)
# MAGIC - Add service principal grants for automation/agents
# MAGIC - Apply row-level or column-level security as needed
# MAGIC
# MAGIC **Note:** You can modify the grants below or skip this cell entirely if permissions are managed separately

# COMMAND ----------

# Grant EXECUTE to all account users (customize as needed)
try:
    spark.sql(f"""
    GRANT EXECUTE ON FUNCTION {catalog}.{functions_schema}.au_calculate_tax
    TO `account users`
    """)
    print("✓ Granted EXECUTE permission to account users")
    print("  You can customize this grant by changing 'account users' to your specific group")
except Exception as e:
    print(f"Note: {e}")
    print("Permissions may need to be set via Account Console")

# COMMAND ----------

# MAGIC %md
# MAGIC ## UC Functions Setup Complete
# MAGIC
# MAGIC All retirement calculation functions are now available as Unity Catalog functions.
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - **02-governance-setup**: Configure row-level security and audit logging
# MAGIC - **02-agent-demo/02-build-agent**: See these functions in action with the agent

# COMMAND ----------

print("✅ Unity Catalog functions setup complete!")
print(f"   Functions created in: {catalog}.{functions_schema}")
print(f"   Total functions: 18 (AU: 3, US: 6, UK: 3, IN: 6)")
print(f"   Tested: au_calculate_tax, us_calculate_401k_tax, uk_check_state_pension, in_calculate_epf_tax")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Member Profiles
# MAGIC
# MAGIC Generate synthetic member data for all 4 countries using Faker

# COMMAND ----------

import random
import pandas as pd
from faker import Faker

# Initialize Faker instances for different locales
fake_au = Faker('en_AU')
fake_us = Faker('en_US')
fake_uk = Faker('en_GB')
fake_in = Faker('en_IN')

# Country-specific configuration
COUNTRY_CONFIG = {
    'AU': {
        'faker': fake_au,
        'preservation_age': 60,
        'member_prefix': 'AU',
        'count': 20,
        'currency_range': (68000, 1250000),
        'income_range': (0, 125000),
        'persona_types': ['Below Average', 'Middle Income', 'Comfortable', 'High Net Worth']
    },
    'US': {
        'faker': fake_us,
        'preservation_age': 59,
        'member_prefix': 'US',
        'count': 20,
        'currency_range': (125000, 580000),
        'income_range': (25000, 95000),
        'persona_types': ['Pre-retirement Planner', 'Wealth Accumulator', 'Retiree Drawdown']
    },
    'UK': {
        'faker': fake_uk,
        'preservation_age': 55,
        'member_prefix': 'UK',
        'count': 20,
        'currency_range': (195000, 280000),
        'income_range': (60000, 75000),
        'persona_types': ['Pre-retirement Planner', 'Wealth Accumulator', 'Retiree Drawdown']
    },
    'IN': {
        'faker': fake_in,
        'preservation_age': 58,
        'member_prefix': 'IN',
        'count': 20,
        'currency_range': (68000, 220000),
        'income_range': (12000, 45000),
        'persona_types': ['Pre-retirement Planner', 'Wealth Accumulator', 'Retiree Drawdown']
    }
}

# Enum values
EMPLOYMENT_STATUS = ['Full-time', 'Part-time', 'Retired', 'Self-employed', 'Unemployed - Health', 'Employed']
FINANCIAL_LITERACY = ['Low', 'Medium', 'Moderate', 'High']
GENDER = ['Male', 'Female']
HEALTH_STATUS = ['Excellent', 'Good', 'Fair', 'Poor', 'Chronic illness']
HOME_OWNERSHIP = ['Owned Outright', 'Homeowner - No Mortgage', 'Homeowner - Mortgage',
                  'Owned with Mortgage', 'Renter', 'Renting', 'Owner']
RISK_PROFILE = ['Conservative', 'Balanced', 'Moderate', 'Growth', 'Aggressive']
MARITAL_STATUS = ['Single', 'Married', 'Divorced', 'Widowed']

print("✓ Configuration loaded")

# COMMAND ----------

# Generate member profiles
members = []

for country, config in COUNTRY_CONFIG.items():
    faker = config['faker']

    for i in range(1, config['count'] + 1):
        # Basic demographics
        age = random.randint(35, 75)
        gender = random.choice(GENDER)
        name = faker.name_male() if gender == 'Male' else faker.name_female()

        # Employment
        is_retired = age >= 65 or random.random() < 0.2
        employment_status = 'Retired' if is_retired else random.choice(['Full-time', 'Part-time', 'Self-employed', 'Employed'])

        # Financials
        super_balance = random.randint(*config['currency_range'])

        if is_retired and age >= config['preservation_age']:
            account_based_pension = random.choice([0, random.randint(25000, 85000)])
        else:
            account_based_pension = 0

        annual_income_outside_super = random.choice([0, random.randint(5000, 25000)]) if is_retired else random.randint(*config['income_range'])

        if age < 50:
            debt = random.randint(30000, 180000)
        elif age < 60:
            debt = random.randint(20000, 95000)
        else:
            debt = random.choice([0, random.randint(0, 50000)])

        dependents = random.randint(0, 3) if age < 45 else (random.randint(0, 2) if age < 60 else random.choice([0, 0, 0, 1]))

        home_ownership = random.choice(['Owned Outright', 'Homeowner - No Mortgage', 'Owner']) if age >= 60 else random.choice(HOME_OWNERSHIP)
        other_assets = random.randint(8000, 450000)

        # Attributes
        persona_type = random.choice(config['persona_types'])
        financial_literacy = random.choice(FINANCIAL_LITERACY)
        health_status = random.choice(HEALTH_STATUS)
        marital_status = random.choice(MARITAL_STATUS)

        if age >= 65:
            risk_profile = random.choice(['Conservative', 'Balanced', 'Moderate'])
        elif age >= 55:
            risk_profile = random.choice(['Balanced', 'Moderate', 'Growth'])
        else:
            risk_profile = random.choice(['Growth', 'Aggressive', 'Balanced'])

        members.append({
            'member_id': f"{config['member_prefix']}{i:03d}",
            'name': name,
            'age': age,
            'country': country,
            'super_balance': super_balance,
            'marital_status': marital_status,
            'other_assets': other_assets,
            'preservation_age': config['preservation_age'],
            'account_based_pension': account_based_pension,
            'annual_income_outside_super': annual_income_outside_super,
            'debt': debt,
            'dependents': dependents,
            'employment_status': employment_status,
            'financial_literacy': financial_literacy,
            'gender': gender,
            'health_status': health_status,
            'home_ownership': home_ownership,
            'persona_type': persona_type,
            'risk_profile': risk_profile
        })

member_profiles_df = pd.DataFrame(members)

print(f"Generated {len(member_profiles_df)} member profiles:")
print(f"  AU: {len(member_profiles_df[member_profiles_df['country'] == 'AU'])}")
print(f"  US: {len(member_profiles_df[member_profiles_df['country'] == 'US'])}")
print(f"  UK: {len(member_profiles_df[member_profiles_df['country'] == 'UK'])}")
print(f"  IN: {len(member_profiles_df[member_profiles_df['country'] == 'IN'])}")

# Preview
display(member_profiles_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Member Profiles to Unity Catalog

# COMMAND ----------

# Convert to Spark DataFrame
spark_member_profiles = spark.createDataFrame(member_profiles_df)

# Write to Delta table
table_name = f"{catalog}.{schema}.member_profiles"
print(f"Writing to {table_name}...")

spark_member_profiles.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✓ Loaded {spark_member_profiles.count()} member profiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Member Profiles

# COMMAND ----------

display(spark.sql(f"""
SELECT
    country,
    COUNT(*) as member_count,
    AVG(age) as avg_age,
    AVG(super_balance) as avg_balance,
    AVG(annual_income_outside_super) as avg_income
FROM {catalog}.{schema}.member_profiles
GROUP BY country
ORDER BY country
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Comprehensive Permissions
# MAGIC
# MAGIC Grant all necessary permissions for Databricks Apps and service principals:
# MAGIC - USE CATALOG on configured catalog
# MAGIC - USE SCHEMA on configured schemas
# MAGIC - SELECT on all tables
# MAGIC - EXECUTE on all UC functions
# MAGIC
# MAGIC **⚠️ Security Note:**
# MAGIC - This example grants permissions to `account users` (all authenticated users in your workspace)
# MAGIC - **For production environments**, grant permissions only to specific users, groups, or service principals who need access
# MAGIC - Replace `account users` with specific principals: `TO user@company.com` or `TO `data_team``
# MAGIC - Example: `GRANT SELECT ON TABLE ... TO `retirement_app_users``

# COMMAND ----------

# Grant catalog-level permissions
try:
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`")
    print(f"✓ Granted USE CATALOG on {catalog}")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# Grant schema-level permissions
try:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `account users`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{functions_schema} TO `account users`")
    print(f"✓ Granted USE SCHEMA on {schema} and {functions_schema}")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# Grant SELECT on all tables
tables = ['member_profiles', 'governance', 'citation_registry']
for table in tables:
    try:
        spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.{table} TO `account users`")
        print(f"✓ Granted SELECT on {table}")
    except Exception as e:
        print(f"Note: {e}")

# COMMAND ----------

# Grant MODIFY on governance table (for audit logging)
try:
    spark.sql(f"GRANT MODIFY ON TABLE {catalog}.{schema}.governance TO `account users`")
    print(f"✓ Granted MODIFY on governance (for audit logging)")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# Grant EXECUTE on all UC functions
functions = spark.sql(f"SHOW USER FUNCTIONS IN {catalog}.{functions_schema}").collect()

for func_row in functions:
    func_name = func_row.function
    try:
        spark.sql(f"GRANT EXECUTE ON FUNCTION {func_name} TO `account users`")
        print(f"✓ Granted EXECUTE on {func_name}")
    except Exception as e:
        print(f"Note: Error granting EXECUTE on {func_name}: {e}")

print(f"\n✓ All permissions granted successfully")
print(f"  Catalog: USE CATALOG on {catalog}")
print(f"  Schemas: USE SCHEMA on {schema} and {functions_schema}")
print(f"  Tables: SELECT on member_profiles, governance, citation_registry")
print(f"  Governance: MODIFY on governance (for audit logging)")
print(f"  Functions: EXECUTE on all {len(functions)} functions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC All Unity Catalog resources created successfully:
# MAGIC - ✅ Catalog and schemas
# MAGIC - ✅ Tables (member_profiles, citation_registry, governance)
# MAGIC - ✅ Citation registry loaded (15 citations)
# MAGIC - ✅ UC functions created (18 functions)
# MAGIC - ✅ Member profiles loaded (29 members)
# MAGIC - ✅ Governance table ready (empty - will populate when agent runs)
# MAGIC
# MAGIC **Next Steps:**
# MAGIC - **01-setup/02-governance-setup**: Configure row-level security and audit logging
# MAGIC - **02-agent-demo/01-agent-overview**: Learn about the agent architecture

# COMMAND ----------

mp_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.member_profiles").collect()[0].cnt
cr_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.citation_registry").collect()[0].cnt
gov_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{schema}.governance").collect()[0].cnt

print("✅ Unity Catalog setup complete!")
print(f"   Catalog: {catalog}")
print(f"   Schemas: {schema}, {functions_schema}")
print(f"   Member profiles: {mp_count}")
print(f"   Citations: {cr_count}")
print(f"   Governance logs: {gov_count} (will populate when agent runs)")
print(f"   UC functions: 18")
print(f"   Ready for agent demo")
