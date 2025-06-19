with a as(SELECT
  user_pseudo_id,
  conversion_flag,
  predicted_conversion_flag,
  predicted_conversion_flag_probs[OFFSET(0)].prob as conversion_probability
FROM
  ML.PREDICT(MODEL `client-tal-ga4.ga4bqml.rfc_propensity_model_no_upid`,
    (SELECT * FROM `client-tal-ga4.ga4bqml.predict_table`)) 

)

SELECT * FROM a 


