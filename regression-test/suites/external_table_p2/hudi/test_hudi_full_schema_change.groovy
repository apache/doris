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

suite("test_hudi_full_schema_change", "p2,external,hudi,external_remote,external_remote_hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
        return
    }

    String catalog_name = "test_hudi_full_schema_change"
    String props = context.config.otherConfigs.get("hudiEmrCatalog")
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            ${props}
        );
    """

    sql """ switch ${catalog_name};"""
    sql """ use regression_hudi;""" 
    sql """ set enable_fallback_to_original_planner=false """
    sql """set force_jni_scanner = false;"""
    


    def tables  = ["hudi_full_schema_change_parquet","hudi_full_schema_change_orc"]


    for (String table: tables) {
        qt_all """ select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} ORDER BY id"""

        qt_country_usa """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'country') = 'USA' ORDER BY id"""
        qt_country_usa_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') AS full_name, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'country') = 'USA' ORDER BY id"""

        qt_city_new """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'city') LIKE 'New%' ORDER BY id"""
        qt_city_new_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') AS age, STRUCT_ELEMENT(array_column[1], 'item') AS first_item FROM ${table} WHERE STRUCT_ELEMENT(struct_column, 'city') LIKE 'New%' ORDER BY id"""

        qt_age_over_30 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') > 30 ORDER BY id"""
        qt_age_over_30_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(array_column[2], 'category') AS second_category FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') > 30 ORDER BY id"""

        qt_age_under_25 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') < 25 ORDER BY id"""
        qt_age_under_25_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, MAP_KEYS(new_map_column)[1] AS map_key FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') < 25 ORDER BY id"""

        qt_name_alice """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') = 'Alice' ORDER BY id"""
        qt_name_alice_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') = 'Alice' ORDER BY id"""

        qt_name_j """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') like 'J%' ORDER BY id"""
        qt_name_j_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'gender') AS gender FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') LIKE 'J%' ORDER BY id"""

        qt_map_person5 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE ARRAY_CONTAINS(MAP_KEYS(new_map_column), 'person5') ORDER BY id"""
        qt_map_person5_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') AS age FROM ${table} WHERE ARRAY_CONTAINS(MAP_KEYS(new_map_column), 'person5') ORDER BY id"""

        qt_array_size_2 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE ARRAY_SIZE(array_column) = 2 ORDER BY id"""
        qt_array_size_2_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'b'), 'cc') AS b_cc FROM ${table} WHERE ARRAY_SIZE(array_column) = 2 ORDER BY id"""

        qt_quantity_not_null """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(array_column[1], 'quantity') IS NOT NULL ORDER BY id"""
        qt_quantity_not_null_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') AS full_name FROM ${table} WHERE STRUCT_ELEMENT(array_column[1], 'quantity') IS NOT NULL ORDER BY id"""

        qt_quantity_null """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(array_column[1], 'quantity') IS NULL ORDER BY id"""
        qt_quantity_null_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(array_column[1], 'quantity') IS NULL ORDER BY id"""

        qt_struct2_not_null """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE struct_column2 IS NOT NULL ORDER BY id"""
        qt_struct2_not_null_cols """select id, STRUCT_ELEMENT(struct_column2, 'c') AS c_value, STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'new_a'), 'new_aa') AS new_aa FROM ${table} WHERE struct_column2 IS NOT NULL ORDER BY id"""

        qt_struct2_null """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE struct_column2 IS NULL ORDER BY id"""
        qt_struct2_null_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city FROM ${table} WHERE struct_column2 IS NULL ORDER BY id"""

        qt_cc_nested """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'b'), 'cc') like 'NestedC%' ORDER BY id"""
        qt_cc_nested_cols """select id, STRUCT_ELEMENT(struct_column2, 'c') AS c_value FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'b'), 'cc') LIKE 'NestedC%' ORDER BY id"""

        qt_c_over_20 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(struct_column2, 'c') > 20 ORDER BY id"""
        qt_c_over_20_cols """select id, STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'b'), 'cc') AS b_cc FROM ${table} WHERE STRUCT_ELEMENT(struct_column2, 'c') > 20 ORDER BY id"""

        qt_new_aa_50 """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'new_a'), 'new_aa') = 50 ORDER BY id"""
        qt_new_aa_50_cols """select id, STRUCT_ELEMENT(struct_column2, 'c') AS c_value FROM ${table} WHERE STRUCT_ELEMENT(STRUCT_ELEMENT(struct_column2, 'new_a'), 'new_aa') = 50 ORDER BY id"""

        qt_gender_female """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'gender') = 'Female' ORDER BY id"""
        qt_gender_female_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, ARRAY_SIZE(array_column) AS array_size FROM ${table} WHERE STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'gender') = 'Female' ORDER BY id"""

        qt_category_fruit """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(array_column[2], 'category') = 'Fruit' ORDER BY id"""
        qt_category_fruit_cols """select id, STRUCT_ELEMENT(struct_column, 'country') AS country, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'full_name') AS full_name FROM ${table} WHERE STRUCT_ELEMENT(array_column[2], 'category') = 'Fruit' ORDER BY id"""

        qt_category_vegetable """select id,new_map_column,struct_column, array_column,struct_column2 FROM ${table} WHERE STRUCT_ELEMENT(array_column[2], 'category') = 'Vegetable' ORDER BY id"""
        qt_category_vegetable_cols """select id, STRUCT_ELEMENT(struct_column, 'city') AS city, STRUCT_ELEMENT(MAP_VALUES(new_map_column)[1], 'age') AS age FROM ${table} WHERE STRUCT_ELEMENT(array_column[2], 'category') = 'Vegetable' ORDER BY id"""
    }
}
/*

spark-sql \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
--conf spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes=false

set hoodie.schema.on.read.enable=true;
set hoodie.metadata.enable=false;
set hoodie.parquet.small.file.limit = 100;


CREATE TABLE hudi_full_schema_change_parquet (
    id int,
    map_column map < string, struct < name: string, age: int > >,
    struct_column struct < city: string, population: int >,
    array_column array < struct < product: string, price: float > >
) USING hudi
OPTIONS (
  type = 'cow',
  primaryKey = 'id'
);


spark-sql (regression_hudi)> select *  from hudi_full_schema_change_parquet order by id;
20250516231815213       20250516231815213_0_0   0               300a99ec-a83a-46e3-8c42-e221a3e4ecb0-0_0-850-897_20250516231815213.parquet      0       {"person0":{"age":2,"full_name":"zero","gender":null}}  {"country":null,"city":"cn"}    [{"item":"Apple","quantity":null,"category":null},{"item":"Banana","quantity":null,"category":null}] NULL
20250516231858211       20250516231858211_0_0   1               198531d4-6ed9-405a-8934-3412e3779f05-0_0-861-907_20250516231858211.parquet      1       {"person1":{"age":25,"full_name":"Alice","gender":null}}        {"country":null,"city":"New York"}      [{"item":"Apple","quantity":null,"category":null},{"item":"Banana","quantity":null,"category":null}] NULL
20250516231902116       20250516231902116_0_0   2               b61f118e-caeb-44fd-876d-1f113a7176bc-0_0-869-914_20250516231902116.parquet      2       {"person2":{"age":30,"full_name":"Bob","gender":null}}  {"country":null,"city":"Los Angeles"}   [{"item":"Orange","quantity":null,"category":null},{"item":"Grape","quantity":null,"category":null}] NULL
20250516231907136       20250516231907136_0_0   3               6ce16dd7-39bd-41d7-a9c1-53626b110466-0_0-877-921_20250516231907136.parquet      3       {"person3":{"age":28,"full_name":"Charlie","gender":null}}      {"country":null,"city":"Chicago"}       [{"item":"Pear","quantity":null,"category":null},{"item":"Mango","quantity":null,"category":null}]   NULL
20250516231911544       20250516231911544_0_0   4               d71ea47c-63fe-4c4b-9178-fd58f8edb6ad-0_0-885-928_20250516231911544.parquet      4       {"person4":{"age":35,"full_name":"David","gender":null}}        {"country":null,"city":"Houston"}       [{"item":"Kiwi","quantity":null,"category":null},{"item":"Pineapple","quantity":null,"category":null}]       NULL
20250516231916994       20250516231916994_0_0   5               65e4615a-f513-4713-a4f6-c94954a028ef-0_0-893-935_20250516231916994.parquet      5       {"person5":{"age":40,"full_name":"Eve","gender":null}}  {"country":"USA","city":"Phoenix"}      [{"item":"Lemon","quantity":null,"category":null},{"item":"Lime","quantity":null,"category":null}]   NULL
20250516231922791       20250516231922791_0_0   6               f358d129-97e1-4b03-b521-64c3c61dce06-0_0-905-946_20250516231922791.parquet      6       {"person6":{"age":22,"full_name":"Frank","gender":null}}        {"country":"USA","city":"Philadelphia"} [{"item":"Watermelon","quantity":null,"category":null},{"item":"Strawberry","quantity":null,"category":null}]        NULL
20250516231927993       20250516231927993_0_0   7               a30b7559-20c2-4f48-8149-f7e53f7fac6b-0_0-917-957_20250516231927993.parquet      7       {"person7":{"age":27,"full_name":"Grace","gender":null}}        {"country":"USA","city":"San Antonio"}  [{"item":"Blueberry","quantity":null,"category":null},{"item":"Raspberry","quantity":null,"category":null}]  NULL
20250516231933098       20250516231933098_0_0   8               a6efa92f-b96c-4479-9350-7f51bbe12f82-0_0-929-968_20250516231933098.parquet      8       {"person8":{"age":32,"full_name":"Hank","gender":null}} {"country":"USA","city":"San Diego"}    [{"item":"Cherry","quantity":5,"category":null},{"item":"Plum","quantity":3,"category":null}]        NULL
20250516231940027       20250516231940027_0_0   9               cdd96b17-42d0-46cc-aa57-91470f824670-0_0-943-981_20250516231940027.parquet      9       {"person9":{"age":29,"full_name":"Ivy","gender":null}}  {"country":"USA","city":"Dallas"}       [{"item":"Peach","quantity":4,"category":null},{"item":"Apricot","quantity":2,"category":null}]      NULL
20250516231944713       20250516231944713_0_0   10              b6bc2558-9978-48e8-b924-b1a8331b8ea0-0_0-955-992_20250516231944713.parquet      10      {"person10":{"age":26,"full_name":"Jack","gender":null}}        {"country":"USA","city":"Austin"}       [{"item":"Fig","quantity":6,"category":null},{"item":"Date","quantity":7,"category":null}]   NULL
20250516231949176       20250516231949176_0_0   11              746d057a-bf94-4ccd-8fc7-80e1bc15b945-0_0-967-1003_20250516231949176.parquet     11      {"person11":{"age":31,"full_name":"Karen","gender":"Female"}}   {"country":"USA","city":"Seattle"}      [{"item":"Coconut","quantity":1,"category":null},{"item":"Papaya","quantity":2,"category":null}]     NULL
20250516231954989       20250516231954989_0_0   12              631edcd5-1e33-44ef-8ae1-5d0ba147cefe-0_0-979-1014_20250516231954989.parquet     12      {"person12":{"age":24,"full_name":"Leo","gender":"Male"}}       {"country":"USA","city":"Portland"}     [{"item":"Guava","quantity":3,"category":null},{"item":"Lychee","quantity":4,"category":null}]       NULL
20250516232000323       20250516232000323_0_0   13              0d676984-e541-48db-afdf-6a265f80f4ca-0_0-991-1025_20250516232000323.parquet     13      {"person13":{"age":33,"full_name":"Mona","gender":"Female"}}    {"country":"USA","city":"Denver"}       [{"item":"Avocado","quantity":2,"category":"Fruit"},{"item":"Tomato","quantity":5,"category":"Vegetable"}]   NULL
20250516232005908       20250516232005908_0_0   14              64daf9fd-b106-413f-bc35-2d4791b2cb40-0_0-1003-1036_20250516232005908.parquet    14      {"person14":{"age":28,"full_name":"Nina","gender":"Female"}}    {"country":"USA","city":"Miami"}        [{"item":"Cucumber","quantity":6,"category":"Vegetable"},{"item":"Carrot","quantity":7,"category":"Vegetable"}]      NULL
20250516232011850       20250516232011850_0_0   15              66e015e2-36d7-485b-bbba-e649a0b61276-0_0-1017-1080_20250516232011850.parquet    15      {"person15":{"age":30,"full_name":"Emma Smith","gender":"Female"}}      {"country":"USA","city":"New York"}     [{"item":"Banana","quantity":3,"category":"Fruit"},{"item":"Potato","quantity":8,"category":"Vegetable"}]    NULL
20250516232016796       20250516232016796_0_0   16              a86f2b41-cf45-40d8-be0a-0c2bf5d657a7-0_0-1029-1091_20250516232016796.parquet    16      {"person16":{"age":28,"full_name":"Liam Brown","gender":"Male"}}        {"country":"UK","city":"London"}        [{"item":"Bread","quantity":2,"category":"Food"},{"item":"Milk","quantity":1,"category":"Dairy"}]    {"b":{"cc":"NestedCC","new_dd":75},"new_a":{"new_aa":50,"bb":"NestedBB"},"c":9}
20250516232022163       20250516232022163_0_0   17              ad21e571-c277-42d6-ad5c-b5801cb81301-0_0-1041-1102_20250516232022163.parquet    17      {"person17":{"age":40,"full_name":"Olivia Davis","gender":"Female"}}    {"country":"Australia","city":"Sydney"} [{"item":"Orange","quantity":4,"category":"Fruit"},{"item":"Broccoli","quantity":6,"category":"Vegetable"}]  {"b":{"cc":"UpdatedCC","new_dd":88},"new_a":{"new_aa":60,"bb":"UpdatedBB"},"c":12}
20250516232027268       20250516232027268_0_0   18              74b620c3-2fef-40de-bad5-db7b36fc2ee4-0_0-1053-1113_20250516232027268.parquet    18      {"person18":{"age":33,"full_name":"Noah Wilson","gender":"Male"}}       {"country":"Germany","city":"Berlin"}   [{"item":"Cheese","quantity":2,"category":"Dairy"},{"item":"Lettuce","quantity":5,"category":"Vegetable"}]   {"b":{"cc":"NestedCC18","new_dd":95},"new_a":{"new_aa":70,"bb":"NestedBB18"},"c":15}
20250516232032532       20250516232032532_0_0   19              f14b6c61-71e2-412e-9709-5cd5d524657e-0_0-1065-1124_20250516232032532.parquet    19      {"person19":{"age":29,"full_name":"Ava Martinez","gender":"Female"}}    {"country":"France","city":"Paris"}     [{"item":"Strawberry","quantity":12,"category":"Fruit"},{"item":"Spinach","quantity":7,"category":"Vegetable"}]      {"b":{"cc":"ReorderedCC","new_dd":101},"new_a":{"new_aa":85,"bb":"ReorderedBB"},"c":18}
20250516232037285       20250516232037285_0_0   20              27b13aa1-b2ad-4e7e-941c-b7d6c404c1bf-0_0-1077-1135_20250516232037285.parquet    20      {"person20":{"age":38,"full_name":"James Lee","gender":"Male"}} {"country":"Japan","city":"Osaka"}      [{"item":"Mango","quantity":6,"category":"Fruit"},{"item":"Onion","quantity":3,"category":"Vegetable"}]      {"b":{"cc":"FinalCC","new_dd":110},"new_a":{"new_aa":95,"bb":"FinalBB"},"c":21}
20250516232043525       20250516232043525_0_0   21              faeb9136-4d2e-4ce7-a6fd-4d648ea3c12d-0_0-1091-1179_20250516232043525.parquet    21      {"person21":{"age":45,"full_name":"Sophia White","gender":"Female"}}    {"country":"Italy","city":"Rome"}       [{"item":"Pasta","quantity":4,"category":"Food"},{"item":"Olive","quantity":9,"category":"Food"}]    {"b":{"cc":"ExampleCC","new_dd":120},"new_a":{"new_aa":100,"bb":"ExampleBB"},"c":25}
Time taken: 1.033 seconds, Fetched 22 row(s)
*/
