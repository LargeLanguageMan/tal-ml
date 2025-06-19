
CREATE OR REPLACE MODEL
  `client-tal-ga4.ga4bqml.rfc_propensity_model_no_upid` OPTIONS( model_type='RANDOM_FOREST_CLASSIFIER',
    data_split_method='AUTO_SPLIT',
    max_tree_depth=20,
    input_label_cols=['conversion_flag'] ) AS
SELECT
  * EXCEPT (user_pseudo_id)
FROM
`client-tal-ga4.ga4bqml.train_table`;