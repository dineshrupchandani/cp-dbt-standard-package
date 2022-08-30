# dbt Package
This package builds a mart of tables and views describing the project it is installed in. In pre V1 versions of the package, the artifacts dbt produces were uploaded to the warehouse, hence the name of the package. That's no longer the case, but the name has stuck!



## Quickstart

1. Add this package to your `packages.yml`:
```
packages:
  - package: brooklyn-data/dbt_artifacts
    version: 1.2.0
```

2. Run `dbt deps` to install the package

3. Add an on-run-end hook to your `dbt_project.yml`: `on-run-end: "{{ dbt_artifacts.upload_results(results) }}"`
(We recommend adding a conditional here so that the upload only occurs in your production environment, such as `on-run-end: "{% if target.name == 'prod' %}{{ dbt_artifacts.upload_results(results) }}{% endif %}"`)

4. Create the tables dbt_artifacts uploads to with `dbt run-operation create_dbt_artifacts_tables`

5. Run your project!

## Configuration

The following configuration can be used to specify where the raw data is uploaded, and where the dbt models are created:

```yml
vars:
  dbt_artifacts_database: your_db # optional, default is your target database
  dbt_artifacts_schema: your_schema # optional, default is your target schema
  dbt_artifacts_create_schema: true|false # optional, set to false if you don't have privileges to create schema, default is true

models:
  ...
  dbt_artifacts:
    +schema: your_destination_schema # optional, default is your target database
    staging:
      +schema: your_destination_schema # optional, default is your target schema
  ...
```

Note that the model materializations are defined in this package's `dbt_project.yml`, so do not set them in your project.

### Environment Variables

If the project is running in dbt Cloud, the following five columns (https://docs.getdbt.com/docs/dbt-cloud/using-dbt-cloud/cloud-environment-variables#special-environment-variables) will be automatically populated in the fct_dbt__invocations model:
- dbt_cloud_project_id
- dbt_cloud_job_id
- dbt_cloud_run_id
- dbt_cloud_run_reason_category
- dbt_cloud_run_reason

To capture other environment variables in the fct_dbt__invocations model in the `env_vars` column, add them to the `env_vars` variable in your `dbt_project.yml`. Note that environment variables with secrets (`DBT_ENV_SECRET_`) can't be logged.
```yml
vars:
  env_vars: [
    'ENV_VAR_1',
    'ENV_VAR_2',
    '...'
  ]
```





