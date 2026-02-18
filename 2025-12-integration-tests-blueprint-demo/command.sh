export DATABRICKS_CONFIG_PROFILE=***

#export DATABRICKS_HOST=https://***
#export DATABRICKS_TOKEN=***
#export DATABRICKS_CLUSTER_ID=***


# Run unit tests
uv sync --only-group unit-tests
uv run python -m pytest -m unit_test

# Run integration tests
uv sync --only-group integration-tests --no-default-groups --no-dev
uv run python -m pytest -m integration_test

