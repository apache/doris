select /*+ SET_VAR(query_timeout = 600) */ ref_1.`i_product_name` as c0, ref_0.`i_category_id` as c1, ref_0.`i_formulation` as c2, ref_0.`i_manager_id` as c3, ref_0.`i_product_name` as c4, ref_1.`i_product_name` as c5, ref_0.`i_size` as c6, avg( cast(42 as int)) over (partition by ref_1.`i_product_name`,ref_1.`i_product_name` order by ref_1.`i_product_name`,ref_1.`i_product_name`) as c7, ref_0.`i_brand` as c8 from regression_test_tpcds_sf1_unique_ck_p1.item as ref_0 inner join regression_test_tpcds_sf1_unique_ck_p1.item as ref_1 on (ref_0.`i_item_desc` = ref_1.`i_product_name` ) where ref_0.`i_product_name` IN ( (select `i_item_id` from regression_test_tpcds_sf1_unique_ck_p1.item limit 187) ) order by ref_0.`i_current_price`