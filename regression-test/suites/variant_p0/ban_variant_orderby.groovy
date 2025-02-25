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

suite("ban_variant_orderby"){

    // case for variant fallback to legacy orderby
    sql """ DROP TABLE IF EXISTS test_v_legacy; """
    sql """  CREATE TABLE test_v_legacy (
              orderid int NOT NULL,
              occur_time array < int > NOT NULL,
              eventType map < int, string > NOT NULL,
              amount struct < f1 : int, f2 : string >,
              j json,
              v variant
            ) ENGINE = OLAP DUPLICATE KEY(orderid) DISTRIBUTED BY HASH(orderid) BUCKETS 4 PROPERTIES ("replication_num" = "1"); """

    sql """ INSERT INTO test_v_legacy VALUES (1, [1, 2, 3], map(1, 'a', 2, 'b'), named_struct('f1', 1, 'f2', 'a'), '{"a": 1}', '2024-02-25 12:30:00'); """
    
    sql """ 
    INSERT INTO test_v_legacy VALUES 
    (2, [1698765433, 1698765450], {3: 'login', 4: 'logout'}, {200, 'EUR'}, '{"user": "Bob", "action": "browse"}', 'hello'),
    (3, [1698765434], {5: 'signup'}, {150, 'GBP'}, '{"user": "Charlie", "action": "signup"}', 'true'),
    (4, [1698765435, 1698765460, 1698765470], {6: 'purchase'}, {300, 'JPY'}, '{"product": "Laptop", "price": 1200}', '[1, 2, 3]'),
    (5, [1698765436], {7: 'cancel', 8: 'refund'}, {50, 'CNY'}, '{"error": "timeout", "retry": true}', '{ "key": "value" }'),
    (6, [1698765437, 1698765480], {9: 'review'}, {180, 'INR'}, '{"rating": 5, "comment": "Excellent!"}', null),
    (7, [1698765438], {}, {120, 'KRW'}, '{"empty": true}', '2024-02-25 12:45:00'),
    (8, [1698765439, 1698765490], {10: 'share', 11: 'like'}, {90, 'AUD'}, '{"post": "news", "likes": 100}', '[false, true]'),
    (9, [1698765440, 1698765500, 1698765510], {12: 'follow'}, {220, 'SGD'}, '{"user": "David", "follows": ["Eve", "Frank"]}', '3.14');
    """

    sql """  INSERT INTO test_v_legacy VALUES (1, [1, 2, 3], map(1, 'a', 2, 'b'), named_struct('f1', 1, 'f2', 'a'), '{"a": 1}', '2024-02-25 12:30:00'); """
    sql """ set enable_fallback_to_original_planner = true """
    sql """ set enable_variant_access_in_original_planner = false """
    qt_sql  "SELECT * FROM test_v_legacy ORDER BY orderid;"
    test {
        sql """  SELECT *, ROW_NUMBER() OVER (     PARTITION BY orderid ORDER BY v DESC) AS row_idfirst FROM test_v_legacy; """
        exception("errCode = 2")
    }

    test {
        sql """  SELECT *, ROW_NUMBER() OVER (     PARTITION BY orderid ORDER BY occur_time ASC) AS row_idfirst FROM test_v_legacy; """
        exception("errCode = 2")
    }

    test {
        sql """  SELECT *, ROW_NUMBER() OVER (     PARTITION BY orderid ORDER BY eventType DESC) AS row_idfirst FROM test_v_legacy; """
        exception("errCode = 2")
    }

    test {
        sql """  SELECT *, ROW_NUMBER() OVER (     PARTITION BY orderid ORDER BY amount ASC) AS row_idfirst FROM test_v_legacy; """
        exception("errCode = 2")
    }
            
}
