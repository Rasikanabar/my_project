
{{ config(materialized='table') }}

select COUNT(*) AS total_issues,
COUNT(DISTINCT VEHICLE_ID) AS affected_vehicles,
STR_TO_DATE(CONCAT(YEAR(REPORTED_AT_PARSED), '-', WEEK(REPORTED_AT_PARSED, 0), '-1'), '%Y-%u-%w') AS issue_week,
COUNT(CASE WHEN state = 'resolved' THEN 1 END) AS resolved_issues,
COUNT(CASE WHEN state = 'open' THEN 1 END) AS open_issues,
MIN(REPORTED_AT_PARSED) AS first_issue_reported,
MAX(REPORTED_AT_PARSED) AS last_issue_reported
from {{ ref('staging') }}
WHERE REPORTED_AT_PARSED IS NOT NULL
GROUP by issue_week
Order by issue_week

