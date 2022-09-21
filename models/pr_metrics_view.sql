{{ config(materialized='view') }}
select
    github_context:job as job_name,
    github_context:actor as triggered_by,
    github_context:base_ref as base_branch,
    github_context:head_ref as featured_branch,
    github_context:event.enterprise.created_at as created_time,
    github_context:event.enterprise.updated_at as updated_time,
    github_context:repository as repository_name,
    github_context:run_id as run_id,
    github_context:event_name as event_name,
    github_context:event.action as event_action_type,
    github_context:event.pull_request.state as state,
   -- '{message}',
    github_context:event.number as event_number,
    github_context:event.pull_request.assignee as Reviewed_by,
    WORKFLOW_START_TIME,
    WORKFLOW_END_TIME,
    concat('[', REGEXP_REPLACE(newly_added_models, '\n', ','), ']') newly_added_models,
    concat('[', REGEXP_REPLACE(modified_models, '\n', ','), ']') modified_models,
    concat('[', REGEXP_REPLACE(dependent_modified_models, '\n', ','), ']') dependent_modified_models,
    concat('[', REGEXP_REPLACE(deleted_models, '\n', ','), ']') deleted_models,
    JOB_STATUS
from "DEV_DBT_TESTING_RAW"."UTIL_COMMON"."PR_METRICS_RAW"