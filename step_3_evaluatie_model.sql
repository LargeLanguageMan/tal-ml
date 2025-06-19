SELECT
   expected_label,
  _0 AS predicted_0,
  _1 AS predicted_1
FROM
  ML.CONFUSION_MATRIX(MODEL `client-tal-ga4.ga4bqml.rfc_propensity_model_no_upid`,
  (
    SELECT
      *
    FROM
      `client-tal-ga4.ga4bqml.train_table`))