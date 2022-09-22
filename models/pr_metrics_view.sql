{{ config(materialized='view')}}

{%- set source_relation = adapter.get_relation(
      database='DEV_DBT_TESTING_RAW',
      schema='UTIL_COMMON',
      identifier='pr_metrics_raw') -%}

{{ log("Source Relation: " ~ source_relation, info=true) }}
{% set table_exists=source_relation is  none   %}
{{ log("table_exists: " ~ table_exists, info=true) }}
{% if table_exists %}

select
    REPLACE(github_context:job, '"', '') as job_name,
    REPLACE(github_context:actor, '"', '') as triggered_by,
    REPLACE(github_context:base_ref, '"', '') as base_branch,
    REPLACE(github_context:head_ref, '"', '') as featured_branch,
    REPLACE(github_context:event.enterprise.created_at, '"', '') as created_time,
    REPLACE(github_context:event.enterprise.updated_at, '"', '') as updated_time,
    REPLACE(github_context:repository, '"', '') as repository_name,
    REPLACE(github_context:run_id, '"', '') as run_id,
    REPLACE(github_context:event_name, '"', '') as event_name,
    REPLACE(github_context:event.action, '"', '') as event_action_type,
    REPLACE(github_context:event.pull_request.state, '"', '') as state,
    REPLACE(github_context:event.commits[array_size(github_context:event.commits) - 1].message, '"', '') as message,
    REPLACE(github_context:event.number, '"', '') as event_number,
    REPLACE(github_context:event.pull_request.assignee, '"', '') as Reviewed_by,
    WORKFLOW_START_TIME,
    WORKFLOW_END_TIME,
    concat('[', REGEXP_REPLACE(newly_added_models, '\n', ','), ']') newly_added_models,
    concat('[', REGEXP_REPLACE(modified_models, '\n', ','), ']') modified_models,
    concat('[', REGEXP_REPLACE(dependent_modified_models, '\n', ','), ']') dependent_modified_models,
    concat('[', REGEXP_REPLACE(deleted_models, '\n', ','), ']') deleted_models,
    JOB_STATUS
from "{{ env_var('ARTIFACTS_DATABASE')}}"."{{ env_var('ARTIFACTS_SCHEMA')}}"."PR_METRICS_RAW"

{% else %}

select
    null::integer as reward_id,
    null::integer as customer_id,
    null::integer as tier

-- this means there will be zero rows
where false

{% endif %}
