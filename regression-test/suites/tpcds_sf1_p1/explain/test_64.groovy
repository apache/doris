// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_regression_test_tpcds_sf1_p1_q64", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  cs_ui AS (
		   SELECT
		     cs_item_sk
		   , sum(cs_ext_list_price) sale
		   , sum(((cr_refunded_cash + cr_reversed_charge) + cr_store_credit)) refund
		   FROM
		     catalog_sales
		   , catalog_returns
		   WHERE (cs_item_sk = cr_item_sk)
		      AND (cs_order_number = cr_order_number)
		   GROUP BY cs_item_sk
		   HAVING (sum(cs_ext_list_price) > (2 * sum(((cr_refunded_cash + cr_reversed_charge) + cr_store_credit))))
		)
		, cross_sales AS (
		   SELECT
		     i_product_name product_name
		   , i_item_sk item_sk
		   , s_store_name store_name
		   , s_zip store_zip
		   , ad1.ca_street_number b_street_number
		   , ad1.ca_street_name b_street_name
		   , ad1.ca_city b_city
		   , ad1.ca_zip b_zip
		   , ad2.ca_street_number c_street_number
		   , ad2.ca_street_name c_street_name
		   , ad2.ca_city c_city
		   , ad2.ca_zip c_zip
		   , d1.d_year syear
		   , d2.d_year fsyear
		   , d3.d_year s2year
		   , count(*) cnt
		   , sum(ss_wholesale_cost) s1
		   , sum(ss_list_price) s2
		   , sum(ss_coupon_amt) s3
		   FROM
		     store_sales
		   , store_returns
		   , cs_ui
		   , date_dim d1
		   , date_dim d2
		   , date_dim d3
		   , store
		   , customer
		   , customer_demographics cd1
		   , customer_demographics cd2
		   , promotion
		   , household_demographics hd1
		   , household_demographics hd2
		   , customer_address ad1
		   , customer_address ad2
		   , income_band ib1
		   , income_band ib2
		   , item
		   WHERE (ss_store_sk = s_store_sk)
		      AND (ss_sold_date_sk = d1.d_date_sk)
		      AND (ss_customer_sk = c_customer_sk)
		      AND (ss_cdemo_sk = cd1.cd_demo_sk)
		      AND (ss_hdemo_sk = hd1.hd_demo_sk)
		      AND (ss_addr_sk = ad1.ca_address_sk)
		      AND (ss_item_sk = i_item_sk)
		      AND (ss_item_sk = sr_item_sk)
		      AND (ss_ticket_number = sr_ticket_number)
		      AND (ss_item_sk = cs_ui.cs_item_sk)
		      AND (c_current_cdemo_sk = cd2.cd_demo_sk)
		      AND (c_current_hdemo_sk = hd2.hd_demo_sk)
		      AND (c_current_addr_sk = ad2.ca_address_sk)
		      AND (c_first_sales_date_sk = d2.d_date_sk)
		      AND (c_first_shipto_date_sk = d3.d_date_sk)
		      AND (ss_promo_sk = p_promo_sk)
		      AND (hd1.hd_income_band_sk = ib1.ib_income_band_sk)
		      AND (hd2.hd_income_band_sk = ib2.ib_income_band_sk)
		      AND (cd1.cd_marital_status <> cd2.cd_marital_status)
		      AND (i_color IN ('purple'   , 'burlywood'   , 'indian'   , 'spring'   , 'floral'   , 'medium'))
		      AND (i_current_price BETWEEN 64 AND (64 + 10))
		      AND (i_current_price BETWEEN (64 + 1) AND (64 + 15))
		   GROUP BY i_product_name, i_item_sk, s_store_name, s_zip, ad1.ca_street_number, ad1.ca_street_name, ad1.ca_city, ad1.ca_zip, ad2.ca_street_number, ad2.ca_street_name, ad2.ca_city, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year
		)
		SELECT
		  cs1.product_name
		, cs1.store_name
		, cs1.store_zip
		, cs1.b_street_number
		, cs1.b_street_name
		, cs1.b_city
		, cs1.b_zip
		, cs1.c_street_number
		, cs1.c_street_name
		, cs1.c_city
		, cs1.c_zip
		, cs1.syear
		, cs1.cnt
		, cs1.s1 s11
		, cs1.s2 s21
		, cs1.s3 s31
		, cs2.s1 s12
		, cs2.s2 s22
		, cs2.s3 s32
		, cs2.syear
		, cs2.cnt
		FROM
		  cross_sales cs1
		, cross_sales cs2
		WHERE (cs1.item_sk = cs2.item_sk)
		   AND (cs1.syear = 1999)
		   AND (cs2.syear = (1999 + 1))
		   AND (cs2.cnt <= cs1.cnt)
		   AND (cs1.store_name = cs2.store_name)
		   AND (cs1.store_zip = cs2.store_zip)
		ORDER BY cs1.product_name ASC, cs1.store_name ASC, cs2.cnt ASC, 14, 15, 16, 17, 18

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 212> `cs1`.`product_name` ASC, <slot 213> `cs1`.`store_name` ASC, <slot 214> `cs2`.`cnt` ASC, <slot 215> `cs1`.`s1` ASC, <slot 216> `cs1`.`s2` ASC, <slot 217> `cs1`.`s3` ASC, <slot 218> `cs2`.`s1` ASC, <slot 219> `cs2`.`s2` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 69> `i_item_sk` = <slot 175> `i_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 70> `s_store_name` = <slot 176> `s_store_name`)\n" + 
				"  |  equal join conjunct: (<slot 71> `s_zip` = <slot 177> `s_zip`)\n" + 
				"  |  other predicates: (<slot 2351> <= <slot 2332>)") && 
		explainStr.contains("other predicates: (<slot 2351> <= <slot 2332>)") && 
		explainStr.contains("vec output tuple id: 83") && 
		explainStr.contains("output slot ids: 1565 1567 1568 1569 1570 1571 1572 1573 1574 1575 1576 1577 1580 1581 1582 1583 1596 1599 1600 1601 1602 \n" + 
				"  |  hash output slot ids: 192 68 70 71 72 73 74 75 76 77 78 79 80 83 84 85 86 186 189 190 191 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 83> count(*)), sum(<slot 84> sum(`ss_wholesale_cost`)), sum(<slot 85> sum(`ss_list_price`)), sum(<slot 86> sum(`ss_coupon_amt`))\n" + 
				"  |  group by: <slot 68> `i_product_name`, <slot 69> `i_item_sk`, <slot 70> `s_store_name`, <slot 71> `s_zip`, <slot 72> `ad1`.`ca_street_number`, <slot 73> `ad1`.`ca_street_name`, <slot 74> `ad1`.`ca_city`, <slot 75> `ad1`.`ca_zip`, <slot 76> `ad2`.`ca_street_number`, <slot 77> `ad2`.`ca_street_name`, <slot 78> `ad2`.`ca_city`, <slot 79> `ad2`.`ca_zip`, <slot 80> `d1`.`d_year`, <slot 81> `d2`.`d_year`, <slot 82> `d3`.`d_year`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 189> count(*)), sum(<slot 190> sum(`ss_wholesale_cost`)), sum(<slot 191> sum(`ss_list_price`)), sum(<slot 192> sum(`ss_coupon_amt`))\n" + 
				"  |  group by: <slot 174> `i_product_name`, <slot 175> `i_item_sk`, <slot 176> `s_store_name`, <slot 177> `s_zip`, <slot 178> `ad1`.`ca_street_number`, <slot 179> `ad1`.`ca_street_name`, <slot 180> `ad1`.`ca_city`, <slot 181> `ad1`.`ca_zip`, <slot 182> `ad2`.`ca_street_number`, <slot 183> `ad2`.`ca_street_name`, <slot 184> `ad2`.`ca_city`, <slot 185> `ad2`.`ca_zip`, <slot 186> `d1`.`d_year`, <slot 187> `d2`.`d_year`, <slot 188> `d3`.`d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*), sum(<slot 1508>), sum(<slot 1509>), sum(<slot 1510>)\n" + 
				"  |  group by: <slot 1540>, <slot 1541>, <slot 1520>, <slot 1521>, <slot 1535>, <slot 1536>, <slot 1537>, <slot 1538>, <slot 1554>, <slot 1555>, <slot 1556>, <slot 1557>, <slot 1523>, <slot 1559>, <slot 1561>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1497> = `ib2`.`ib_income_band_sk`)") && 
		explainStr.contains("vec output tuple id: 82") && 
		explainStr.contains("output slot ids: 1508 1509 1510 1520 1521 1523 1535 1536 1537 1538 1540 1541 1554 1555 1556 1557 1559 1561 \n" + 
				"  |  hash output slot ids: 1505 1479 1480 1481 1482 1452 1484 1453 1485 1454 1464 1465 1498 1467 1499 1500 1501 1503 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1423> = `ib1`.`ib_income_band_sk`)") && 
		explainStr.contains("vec output tuple id: 81") && 
		explainStr.contains("output slot ids: 1452 1453 1454 1464 1465 1467 1479 1480 1481 1482 1484 1485 1497 1498 1499 1500 1501 1503 1505 \n" + 
				"  |  hash output slot ids: 1409 1410 1442 1443 1412 1444 1445 1446 1448 1450 1424 1425 1426 1427 1397 1429 1398 1430 1399 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1366> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 80") && 
		explainStr.contains("output slot ids: 1397 1398 1399 1409 1410 1412 1423 1424 1425 1426 1427 1429 1430 1442 1443 1444 1445 1446 1448 1450 \n" + 
				"  |  hash output slot ids: 1344 1376 1345 1377 1346 134 1356 1357 1389 1390 1359 1391 1392 1393 1395 1370 1371 1372 1373 1374 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1314> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 79") && 
		explainStr.contains("output slot ids: 1344 1345 1346 1356 1357 1359 1366 1370 1371 1372 1373 1374 1376 1377 1389 1390 1391 1392 1393 1395 \n" + 
				"  |  hash output slot ids: 1315 133 1319 1320 1321 1322 1323 1293 1325 1294 1326 1295 1305 1306 1338 1339 1308 1340 1341 1342 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1267> = `ad2`.`ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 78") && 
		explainStr.contains("output slot ids: 1293 1294 1295 1305 1306 1308 1314 1315 1319 1320 1321 1322 1323 1325 1326 1338 1339 1340 1341 1342 \n" + 
				"  |  hash output slot ids: 1248 1280 128 1249 129 130 131 1259 1260 1292 1262 1268 1269 1273 1274 1275 1276 1277 1247 1279 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1222> = `hd2`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 77") && 
		explainStr.contains("output slot ids: 1247 1248 1249 1259 1260 1262 1267 1268 1269 1273 1274 1275 1276 1277 1279 1280 1292 \n" + 
				"  |  hash output slot ids: 1216 1218 1223 1224 168 1225 1229 1230 1231 1232 1233 1203 1235 1204 1236 1205 1215 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1179> = `cd2`.`cd_demo_sk`)\n" + 
				"  |  other predicates: (<slot 2181> != <slot 2190>)") && 
		explainStr.contains("other predicates: (<slot 2181> != <slot 2190>)") && 
		explainStr.contains("vec output tuple id: 76") && 
		explainStr.contains("output slot ids: 1203 1204 1205 1215 1216 1218 1222 1223 1224 1225 1229 1230 1231 1232 1233 1235 1236 \n" + 
				"  |  hash output slot ids: 1185 1187 1188 1189 1190 1191 1161 1193 1162 1194 1163 171 1173 1174 1176 1180 1181 1182 1183 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1131> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 75") && 
		explainStr.contains("output slot ids: 1161 1162 1163 1173 1174 1176 1179 1180 1181 1182 1183 1185 1187 1188 1189 1190 1191 1193 1194 \n" + 
				"  |  hash output slot ids: 1120 1152 1121 1153 1122 1132 1133 1135 1138 1139 1140 1141 1142 1144 1146 1147 1148 1149 1150 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1083> = <slot 114> `cs_item_sk`)") && 
		explainStr.contains("vec output tuple id: 74") && 
		explainStr.contains("output slot ids: 1120 1121 1122 1131 1132 1133 1135 1138 1139 1140 1141 1142 1144 1146 1147 1148 1149 1150 1152 1153 \n" + 
				"  |  hash output slot ids: 1089 1092 1093 1094 1095 1096 1098 1100 1101 1102 1103 1104 1074 1106 1075 1107 1076 1085 1086 1087 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1047> = `sr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 1048> = `sr_ticket_number`)") && 
		explainStr.contains("vec output tuple id: 72") && 
		explainStr.contains("output slot ids: 1074 1075 1076 1083 1085 1086 1087 1089 1092 1093 1094 1095 1096 1098 1100 1101 1102 1103 1104 1106 1107 \n" + 
				"  |  hash output slot ids: 1056 1057 1058 1059 1060 1062 1064 1065 1066 1067 1068 1038 1070 1039 1071 1040 1047 1049 1050 1051 1053 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 1015> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 71") && 
		explainStr.contains("output slot ids: 1038 1039 1040 1047 1048 1049 1050 1051 1053 1056 1057 1058 1059 1060 1062 1064 1065 1066 1067 1068 1070 1071 \n" + 
				"  |  hash output slot ids: 1024 1025 1026 1027 1028 1030 1032 1033 1034 1035 1036 1006 1007 1008 1015 1016 120 1017 121 1018 1019 1021 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 987> = `ad1`.`ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 70") && 
		explainStr.contains("output slot ids: 1006 1007 1008 1015 1016 1017 1018 1019 1021 1024 1025 1026 1027 1028 1030 1032 1033 1034 1035 1036 \n" + 
				"  |  hash output slot ids: 992 994 997 998 999 1000 1001 1003 1005 979 980 981 988 124 989 125 990 126 991 127 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 961> = `hd1`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 69") && 
		explainStr.contains("output slot ids: 979 980 981 987 988 989 990 991 992 994 997 998 999 1000 1001 1003 1005 \n" + 
				"  |  hash output slot ids: 962 963 964 965 966 166 967 969 972 973 974 975 976 978 954 955 956 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 937> = `cd1`.`cd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 68") && 
		explainStr.contains("output slot ids: 954 955 956 961 962 963 964 965 966 967 969 972 973 974 975 976 978 \n" + 
				"  |  hash output slot ids: 931 932 933 938 170 939 940 941 942 943 944 946 949 950 951 952 953 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 919> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 67") && 
		explainStr.contains("output slot ids: 931 932 933 937 938 939 940 941 942 943 944 946 949 950 951 952 953 \n" + 
				"  |  hash output slot ids: 160 929 162 914 915 916 920 921 922 154 923 924 156 925 926 158 927 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 903> = `d1`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 66") && 
		explainStr.contains("output slot ids: 914 915 916 919 920 921 922 923 924 925 926 927 929 \n" + 
				"  |  hash output slot ids: 899 900 132 901 904 905 906 907 908 909 910 911 912 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_store_sk` = `s_store_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `s_store_sk`") && 
		explainStr.contains("vec output tuple id: 65") && 
		explainStr.contains("output slot ids: 899 900 901 903 904 905 906 907 908 909 910 911 912 \n" + 
				"  |  hash output slot ids: 164 135 136 137 140 142 144 146 148 150 152 122 123 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ss_store_sk`") && 
		explainStr.contains("TABLE: income_band(income_band), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: income_band(income_band), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 115> sum(`cs_ext_list_price`)), sum(<slot 116> sum(((`cr_refunded_cash` + `cr_reversed_charge`) + `cr_store_credit`)))\n" + 
				"  |  group by: <slot 114> `cs_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 1113>), sum(((<slot 1115> + <slot 1116>) + <slot 1117>))\n" + 
				"  |  group by: <slot 1112>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `cr_item_sk`)\n" + 
				"  |  equal join conjunct: (`cs_order_number` = `cr_order_number`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `cr_item_sk`, RF005[in_or_bloom] <- `cr_order_number`") && 
		explainStr.contains("vec output tuple id: 73") && 
		explainStr.contains("output slot ids: 1112 1113 1115 1116 1117 \n" + 
				"  |  hash output slot ids: 106 107 108 109 110 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `cs_item_sk`, RF005[in_or_bloom] -> `cs_order_number`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_color` IN ('purple', 'burlywood', 'indian', 'spring', 'floral', 'medium')), `i_current_price` >= 64, `i_current_price` <= 74, `i_current_price` >= 65, `i_current_price` <= 79") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d1`.`d_year` = 2000)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*), sum(<slot 842>), sum(<slot 843>), sum(<slot 844>)\n" + 
				"  |  group by: <slot 874>, <slot 875>, <slot 854>, <slot 855>, <slot 869>, <slot 870>, <slot 871>, <slot 872>, <slot 888>, <slot 889>, <slot 890>, <slot 891>, <slot 857>, <slot 893>, <slot 895>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 831> = `ib2`.`ib_income_band_sk`)") && 
		explainStr.contains("vec output tuple id: 64") && 
		explainStr.contains("output slot ids: 842 843 844 854 855 857 869 870 871 872 874 875 888 889 890 891 893 895 \n" + 
				"  |  hash output slot ids: 832 801 833 834 835 837 839 813 814 815 816 786 818 787 819 788 798 799 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 757> = `ib1`.`ib_income_band_sk`)") && 
		explainStr.contains("vec output tuple id: 63") && 
		explainStr.contains("output slot ids: 786 787 788 798 799 801 813 814 815 816 818 819 831 832 833 834 835 837 839 \n" + 
				"  |  hash output slot ids: 743 744 776 777 746 778 779 780 782 784 758 759 760 761 731 763 732 764 733 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 700> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 62") && 
		explainStr.contains("output slot ids: 731 732 733 743 744 746 757 758 759 760 761 763 764 776 777 778 779 780 782 784 \n" + 
				"  |  hash output slot ids: 704 705 706 707 708 678 710 679 711 680 690 691 723 724 693 725 726 727 729 28 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 648> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 61") && 
		explainStr.contains("output slot ids: 678 679 680 690 691 693 700 704 705 706 707 708 710 711 723 724 725 726 727 729 \n" + 
				"  |  hash output slot ids: 640 672 673 642 674 675 676 649 653 654 655 656 657 627 659 628 660 629 27 639 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 601> = `ad2`.`ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 60") && 
		explainStr.contains("output slot ids: 627 628 629 639 640 642 648 649 653 654 655 656 657 659 660 672 673 674 675 676 \n" + 
				"  |  hash output slot ids: 608 609 610 611 581 613 582 614 583 593 594 626 596 22 23 24 25 602 603 607 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 556> = `hd2`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 59") && 
		explainStr.contains("output slot ids: 581 582 583 593 594 596 601 602 603 607 608 609 610 611 613 614 626 \n" + 
				"  |  hash output slot ids: 549 550 552 557 558 559 563 564 565 566 567 537 569 538 570 539 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 513> = `cd2`.`cd_demo_sk`)\n" + 
				"  |  other predicates: (<slot 1824> != <slot 1833>)") && 
		explainStr.contains("other predicates: (<slot 1824> != <slot 1833>)") && 
		explainStr.contains("vec output tuple id: 58") && 
		explainStr.contains("output slot ids: 537 538 539 549 550 552 556 557 558 559 563 564 565 566 567 569 570 \n" + 
				"  |  hash output slot ids: 65 514 515 516 517 519 521 522 523 524 525 495 527 496 528 497 507 508 510 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 465> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 57") && 
		explainStr.contains("output slot ids: 495 496 497 507 508 510 513 514 515 516 517 519 521 522 523 524 525 527 528 \n" + 
				"  |  hash output slot ids: 480 481 482 483 484 454 486 455 487 456 466 467 469 472 473 474 475 476 478 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 417> = <slot 8> `cs_item_sk`)") && 
		explainStr.contains("vec output tuple id: 56") && 
		explainStr.contains("output slot ids: 454 455 456 465 466 467 469 472 473 474 475 476 478 480 481 482 483 484 486 487 \n" + 
				"  |  hash output slot ids: 419 420 421 423 426 427 428 429 430 432 434 435 436 437 438 408 440 409 441 410 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 381> = `sr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 382> = `sr_ticket_number`)") && 
		explainStr.contains("vec output tuple id: 54") && 
		explainStr.contains("output slot ids: 408 409 410 417 419 420 421 423 426 427 428 429 430 432 434 435 436 437 438 440 441 \n" + 
				"  |  hash output slot ids: 384 385 387 390 391 392 393 394 396 398 399 400 401 402 372 404 373 405 374 381 383 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 349> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 53") && 
		explainStr.contains("output slot ids: 372 373 374 381 382 383 384 385 387 390 391 392 393 394 396 398 399 400 401 402 404 405 \n" + 
				"  |  hash output slot ids: 352 353 355 358 359 360 361 362 364 366 14 367 15 368 369 370 340 341 342 349 350 351 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 321> = `ad1`.`ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 52") && 
		explainStr.contains("output slot ids: 340 341 342 349 350 351 352 353 355 358 359 360 361 362 364 366 367 368 369 370 \n" + 
				"  |  hash output slot ids: 322 323 324 325 326 328 331 332 333 334 335 337 18 339 19 20 21 313 314 315 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 295> = `hd1`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 51") && 
		explainStr.contains("output slot ids: 313 314 315 321 322 323 324 325 326 328 331 332 333 334 335 337 339 \n" + 
				"  |  hash output slot ids: 288 289 290 296 297 298 299 300 301 303 306 307 308 309 310 312 60 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 271> = `cd1`.`cd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 50") && 
		explainStr.contains("output slot ids: 288 289 290 295 296 297 298 299 300 301 303 306 307 308 309 310 312 \n" + 
				"  |  hash output slot ids: 64 265 266 267 272 273 274 275 276 277 278 280 283 284 285 286 287 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 253> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 49") && 
		explainStr.contains("output slot ids: 265 266 267 271 272 273 274 275 276 277 278 280 283 284 285 286 287 \n" + 
				"  |  hash output slot ids: 256 257 258 259 260 261 263 48 50 52 54 248 56 249 250 254 255 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 237> = `d1`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 48") && 
		explainStr.contains("output slot ids: 248 249 250 253 254 255 256 257 258 259 260 261 263 \n" + 
				"  |  hash output slot ids: 233 234 235 238 239 240 241 242 243 244 245 246 26 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_store_sk` = `s_store_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `s_store_sk`") && 
		explainStr.contains("vec output tuple id: 47") && 
		explainStr.contains("output slot ids: 233 234 235 237 238 239 240 241 242 243 244 245 246 \n" + 
				"  |  hash output slot ids: 34 36 38 40 42 44 46 16 17 58 29 30 31 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_store_sk`") && 
		explainStr.contains("TABLE: income_band(income_band), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: income_band(income_band), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 9> sum(`cs_ext_list_price`)), sum(<slot 10> sum(((`cr_refunded_cash` + `cr_reversed_charge`) + `cr_store_credit`)))\n" + 
				"  |  group by: <slot 8> `cs_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 447>), sum(((<slot 449> + <slot 450>) + <slot 451>))\n" + 
				"  |  group by: <slot 446>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `cr_item_sk`)\n" + 
				"  |  equal join conjunct: (`cs_order_number` = `cr_order_number`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `cr_item_sk`, RF002[in_or_bloom] <- `cr_order_number`") && 
		explainStr.contains("vec output tuple id: 55") && 
		explainStr.contains("output slot ids: 446 447 449 450 451 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_item_sk`, RF002[in_or_bloom] -> `cs_order_number`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_color` IN ('purple', 'burlywood', 'indian', 'spring', 'floral', 'medium')), `i_current_price` >= 64, `i_current_price` <= 74, `i_current_price` >= 65, `i_current_price` <= 79") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d1`.`d_year` = 1999)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") 
            
        }
    }
}