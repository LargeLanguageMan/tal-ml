CREATE OR REPLACE TABLE `ga4bqml.predict_table` as

WITH form_complete AS (SELECT
  user_pseudo_id,
  MAX(CASE 
  WHEN event_name IN ('form_complete', 'talc_quote_complete_u50', 'talc_quote_complete_o50') THEN 1 ELSE 0 END) AS conversion_flag
FROM
  `client-tal-ga4.analytics_339341518.events_*`
WHERE
_TABLE_SUFFIX >= '20250331'


GROUP BY
  user_pseudo_id
  ),

demographics AS (
  SELECT
    user_pseudo_id,
    device.category as device_category,
    device.operating_system as operating_system,
    platform as platform,
    geo.city as city,
    device.web_info.browser as browser,
    traffic_source.medium as traffic_source,
    device.mobile_brand_name as device_brand,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS row_num
  FROM `client-tal-ga4.analytics_339341518.events_*`
  WHERE event_name = "user_engagement"
  AND _TABLE_SUFFIX >= '20250331'


),

behavioural as(
SELECT
  user_pseudo_id,
  SUM(IF(event_name = 'download', 1, 0)) AS cnt_download,
  SUM(IF(event_name = 'form_step', 1, 0)) AS cnt_form_step,
  SUM(IF(form_step_life_plan IS NOT NULL, 1, 0)) AS cnt_form_step_life_plan,
  SUM(IF(form_step_income_protection IS NOT NULL, 1, 0)) AS cnt_form_step_income_protection,
  SUM(IF(form_step_duty_of_disclosure IS NOT NULL, 1, 0)) AS cnt_form_step_duty_of_disclosure,
  SUM(IF(form_step_personal_details IS NOT NULL, 1, 0)) AS cnt_form_step_personal_details,
  SUM(IF(form_step_additional_information IS NOT NULL, 1, 0)) AS cnt_form_step_additional_information,
  SUM(IF(form_step_assessment_summary IS NOT NULL, 1, 0)) AS cnt_form_step_assessment_summary,
  SUM(IF(form_step_policy_owner_details IS NOT NULL, 1, 0)) AS cnt_form_step_policy_owner_details,
  SUM(IF(form_step_commencement_commissions IS NOT NULL, 1, 0)) AS cnt_form_step_commencement_commissions,
  SUM(IF(form_step_policy_declaration IS NOT NULL, 1, 0)) AS cnt_form_step_policy_declaration,
  SUM(IF(form_step_medical_consent IS NOT NULL, 1, 0)) AS cnt_form_step_medical_consent,
  SUM(IF(form_step_submission_instructions IS NOT NULL, 1, 0)) AS cnt_form_step_submission_instructions,
  SUM(IF(form_step_submit_application IS NOT NULL, 1, 0)) AS cnt_form_step_submit_application,
  SUM(IF(form_step_standalone_ci IS NOT NULL, 1, 0)) AS cnt_form_step_standalone_ci,
  SUM(IF(form_step_additional_life IS NOT NULL, 1, 0)) AS cnt_form_step_additional_life,
  SUM(IF(event_name = 'page_view', 1, 0)) AS cnt_page_views,
  COUNT(DISTINCT session_id) as cnt_sessions,
  SUM(IF(event_name = 'site_search', 1, 0)) AS cnt_site_search,
    SUM(IF(form_step_about_you IS NOT NULL, 1, 0)) AS cnt_form_step_about_you,
  SUM(IF(form_step_select_cover IS NOT NULL, 1, 0)) AS cnt_form_step_select_cover,
  SUM(IF(form_step_step_2 IS NOT NULL, 1, 0)) AS cnt_form_step_step_2,
  SUM(IF(form_step_pay_amount IS NOT NULL, 1, 0)) AS cnt_form_step_pay_amount,
  SUM(IF(form_step_introduction IS NOT NULL, 1, 0)) AS cnt_form_step_introduction,
  SUM(IF(form_step_review_cover IS NOT NULL, 1, 0)) AS cnt_form_step_review_cover,


  FROM (
    SELECT 
      user_pseudo_id,
      event_name,
      CONCAT(user_pseudo_id, (SELECT value.int_value FROM UNNEST (event_params) WHERE key = 'ga_session_id')) AS session_id,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'life plan') as form_step_life_plan,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'income protection') as form_step_income_protection,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'duty of disclosure') as form_step_duty_of_disclosure,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'personal details') as form_step_personal_details,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'additional information') as form_step_additional_information,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'assessment summary') as form_step_assessment_summary,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'policy owner details') as form_step_policy_owner_details,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'commencement & commissions') as form_step_commencement_commissions,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'policy declaration and authority') as form_step_policy_declaration,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'medical consent authority') as form_step_medical_consent,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'application submission instructions') as form_step_submission_instructions,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'submit application') as form_step_submit_application,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'stand-alone ci') as form_step_standalone_ci,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'additional life') as form_step_additional_life,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'about you') as form_step_about_you,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'select cover') as form_step_select_cover,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'step-2') as form_step_step_2,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'pay amount') as form_step_pay_amount,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'introduction') as form_step_introduction,
      (SELECT value.string_value FROM UNNEST (event_params) WHERE key = 'form_step' AND value.string_value = 'review cover') as form_step_review_cover


            
    FROM `client-tal-ga4.analytics_339341518.events_*`
  WHERE _TABLE_SUFFIX >= '20250331'
  GROUP BY 
    user_pseudo_id,
    event_name,
    event_params
  )

  group by user_pseudo_id


)


SELECT
fc.conversion_flag as conversion_flag,
dem.* EXCEPT (row_num),
IFNULL(cnt_page_views,0) as cnt_page_views,
IFNULL(cnt_sessions,0) as cnt_sessions,
-- IFNULL(beh.average_session_duration,0) AS average_session_duration,
IFNULL(cnt_site_search,0) cnt_site_search,
IFNULL(beh.cnt_download,0) AS cnt_download,
IFNULL(beh.cnt_form_step,0) AS cnt_form_step,
IFNULL(beh.cnt_form_step_life_plan,0) AS cnt_form_step_life_plan,
IFNULL(beh.cnt_form_step_income_protection,0) AS cnt_form_step_income_protection,
IFNULL(beh.cnt_form_step_duty_of_disclosure,0) AS cnt_form_step_duty_of_disclosure,
IFNULL(beh.cnt_form_step_personal_details,0) AS cnt_form_step_personal_details,
IFNULL(beh.cnt_form_step_additional_information,0) AS cnt_form_step_additional_information,
IFNULL(beh.cnt_form_step_assessment_summary,0) AS cnt_form_step_assessment_summary,
IFNULL(beh.cnt_form_step_policy_owner_details,0) AS cnt_form_step_policy_owner_details,
IFNULL(beh.cnt_form_step_commencement_commissions,0) AS cnt_form_step_commencement_commissions,
IFNULL(beh.cnt_form_step_policy_declaration,0) AS cnt_form_step_policy_declaration,
IFNULL(beh.cnt_form_step_medical_consent,0) AS cnt_form_step_medical_consent,
IFNULL(beh.cnt_form_step_submission_instructions,0) AS cnt_form_step_submission_instructions,
IFNULL(beh.cnt_form_step_submit_application,0) AS cnt_form_step_submit_application,
IFNULL(beh.cnt_form_step_standalone_ci,0) AS cnt_form_step_standalone_ci,


from 
form_complete fc 
LEFT OUTER JOIN
demographics dem
on fc.user_pseudo_id = dem.user_pseudo_id
LEFT OUTER JOIN
behavioural beh
on fc.user_pseudo_id = beh.user_pseudo_id
WHERE row_num = 1
AND conversion_flag = 0 -- this is to exclude anyone that has converted, as we're only interested in those that did not convert
--FEATURE ENGINEERING
and dem.city IN ('Sydney','Brisbane','Melbourne','Perth')
and dem.device_brand IN ('Google','Apple','Microsoft','Mozilla')

