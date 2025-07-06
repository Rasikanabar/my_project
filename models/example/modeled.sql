
-- Use the `ref` function to select from other models
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



/*SELECT
  DATE_TRUNC('WEEK', CAST(REPORTED_AT AS DATE)) AS issue_week,
  COUNT(*) AS total_issues,
  COUNTIF(STATE = 'resolved') AS resolved_issues,
  COUNTIF(STATE = 'open') AS open_issues,
  COUNT(DISTINCT VEHICLE_ID) AS affected_vehicles,
  MIN(REPORTED_AT) AS first_issue_reported,
  MAX(REPORTED_AT) AS last_issue_reported
FROM {{ ref('staging') }}
WHERE REPORTED_AT IS NOT NULL
GROUP BY 1
ORDER BY 1*/
