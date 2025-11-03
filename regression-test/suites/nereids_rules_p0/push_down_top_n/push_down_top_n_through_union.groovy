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

suite("push_down_top_n_through_union") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql """
        DROP TABLE IF EXISTS table1;
    """

    sql """
    CREATE TABLE IF NOT EXISTS table1(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    qt_push_down_topn_through_union """
        explain shape plan select * from (select * from table1 t1 union all select * from table1 t2) t order by id limit 10;
    """

    qt_push_down_topn_union_with_conditions """
        explain shape plan select * from (select * from table1 t1 where t1.score > 10 union all select * from table1 t2 where t2.name = 'Test' union all select * from table1 t3 where t3.id < 5) sub order by id limit 10;
    """

    qt_push_down_topn_union_with_order_by """
        explain shape plan select * from (select * from table1 t1 union all select * from table1 t2 union all select * from table1 t3 order by score) sub order by id limit 10;
    """

    qt_push_down_topn_nested_union """
        explain shape plan select * from ((select * from table1 t1 union all select * from table1 t2) union all (select * from table1 t3 union all select * from table1 t4)) sub order by id limit 10;
    """

    qt_push_down_topn_union_after_join """
        explain shape plan select * from (select t1.id from table1 t1 join table1 t2 on t1.id = t2.id union all select id from table1 t3) sub order by id limit 10;
    """

    qt_push_down_topn_union_different_projections """
        explain shape plan select * from (select id from table1 t1 union all select name from table1 t2) sub order by id limit 10;
    """

    qt_push_down_topn_union_with_subquery """
        explain shape plan select * from (select id from (select * from table1 where score > 20) t1 union all select id from table1 t2) sub order by id limit 10;
    """

    qt_push_down_topn_union_with_limit """
        explain shape plan select * from ((select * from table1 t1 limit 5) union all (select * from table1 t2 limit 5)) sub order by id limit 10;
    """

    qt_push_down_topn_union_complex_conditions """
        explain shape plan select * from (select * from table1 t1 where t1.score > 10 and t1.name = 'Test' union all select * from table1 t2 where t2.id < 5 and t2.score < 20) sub order by id limit 10;
    """


    sql "DROP TABLE IF EXISTS t1";

    sql """
    CREATE TABLE `t1` (

    `logTimestamp` datetime NULL,

    `args1` varchar(65533) NULL,

    `args2` variant NULL,

    `args3` int NULL,

    `args4` text NULL,

    `args5` varchar(65533) NULL,

    `args6` varchar(65533) NULL,

    `args7` varchar(200) NULL,

    `args8` varchar(65533) NULL,

    `args9` variant NULL,

    `args10` variant NULL,

    `args11` variant NULL,

    `args12` variant NULL,

    `args13` varchar(65533) NULL,

    `args14` text NULL,

    `args15` variant NULL,

    `log` variant NULL
    ) ENGINE=OLAP

    DISTRIBUTED BY RANDOM BUCKETS 10

    PROPERTIES (

            "replication_num" = "1"

    );"""

    sql """
    INSERT INTO t1 (
            `logTimestamp`, `args1`, `args2`, `args3`, `args4`,
            `args5`, `args6`, `args7`, `args8`, `args9`,
            `args10`, `args11`, `args12`, `args13`, `args14`,
            `args15`, `log`
    ) VALUES

    ('2025-07-31 09:15:22', 'username=admin&password=*****', '{"action":"login","status":"success"}', 200, 'User authentication successful',
    '192.168.1.100', '/api/v1/auth/login', '192.168.1.100', 'auth.example.com', '{"app":"authentication","env":"production"}',
    '{"country":"CN","city":"Beijing","coordinates":{"lat":39.9042,"lon":116.4074}}', '{"Content-Type":"application/json","Authorization":"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}', '{"server":"auth-node-01","region":"east-1"}', 'https://portal.example.com/login', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    '{"source":"web","tags":["security","auth"]}', '{"timestamp":"2025-07-31T09:15:22Z","level":"INFO","message":"Successful login for user admin","context":{"ip":"192.168.1.100","user_agent":"Chrome/125.0.0.0","session_id":"sess_xyz123"}}'),


    ('2025-07-31 11:30:45', 'product_id=789&quantity=2&price=59.99', '{"event":"purchase","payment_method":"credit_card"}', 201, 'Order processed successfully',
    '192.168.1.101', '/api/v1/orders', '192.168.1.101', 'shop.example.com', '{"app":"ecommerce","env":"production","version":"3.2.1"}',
    '{"country":"US","city":"New York","coordinates":{"lat":40.7128,"lon":-74.0060}}', '{"Content-Type":"application/json","X-CSRF-Token":"csrf_abc123","Accept":"application/json"}', '{"server":"shop-node-03","region":"us-east-1"}', 'https://shop.example.com/product/789', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1',
    '{"source":"mobile","tags":["transaction","order"]}', '{"timestamp":"2025-07-31T11:30:45Z","level":"INFO","message":"New order created","context":{"order_id":"ord_789xyz","amount":119.98,"currency":"USD","customer_id":"cust_123abc"}}'),


    ('2025-07-31 14:20:18', 'transaction_id=txn_987654&amount=199.99', '{"type":"payment","gateway":"stripe","currency":"USD"}', 202, 'Payment processing started',
    '192.168.1.102', '/api/v1/payments', '192.168.1.102', 'pay.example.com', '{"app":"payment","env":"production","features":["3d_secure","refunds"]}',
    '{"country":"GB","city":"London","coordinates":{"lat":51.5074,"lon":-0.1278}}', '{"Content-Type":"application/json","X-Request-ID":"req_987xyz","Idempotency-Key":"idemp_123"}', '{"server":"payment-gw-02","region":"eu-west-2"}', 'https://checkout.example.com/payment', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    '{"source":"web","tags":["finance","payment"]}', '{"timestamp":"2025-07-31T14:20:18Z","level":"INFO","message":"Payment initiated","context":{"transaction_id":"txn_987654","amount":199.99,"currency":"USD","customer":"cust_456def"}}'),


    ('2025-07-31 16:45:33', 'error_code=500&message=Database+connection+failed', '{"severity":"critical","component":"database"}', 500, 'Database connection error',
    '192.168.1.103', '/api/v1/products', '192.168.1.103', 'api.example.com', '{"app":"api-gateway","env":"production","dependencies":["mysql","redis"]}',
    '{"country":"JP","city":"Tokyo","coordinates":{"lat":35.6762,"lon":139.6503}}', '{"Content-Type":"application/json","X-Correlation-ID":"corr_456mno"}', '{"server":"api-node-05","region":"ap-northeast-1"}', 'https://app.example.com/products', 'Mozilla/5.0 (Linux; Android 13; SM-S901B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36',
    '{"source":"mobile","tags":["error","database"]}', '{"timestamp":"2025-07-31T16:45:33Z","level":"ERROR","message":"Failed to connect to database","context":{"error":"Connection timeout","attempts":3,"database":"products_db","duration_ms":5000}}'),


    ('2025-07-31 19:05:07', 'query=smartphone&page=1&sort=price_asc', '{"search_type":"product","filters":["in_stock","free_shipping"]}', 200, 'Search results returned',
    '192.168.1.104', '/api/v1/search', '192.168.1.104', 'search.example.com', '{"app":"search","env":"production","features":["autocomplete","faceted_search"]}',
    '{"country":"DE","city":"Berlin","coordinates":{"lat":52.5200,"lon":13.4050}}', '{"Content-Type":"application/json","X-Forwarded-For":"192.168.1.104, 203.0.113.5"}', '{"server":"search-node-01","region":"eu-central-1"}', 'https://www.example.com/search', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/125.0',
    '{"source":"web","tags":["search","query"]}', '{"timestamp":"2025-07-31T19:05:07Z","level":"INFO","message":"Search query processed","context":{"query":"smartphone","results_count":42,"response_time_ms":125,"filters_applied":2}}');
   """

    sql """
    DROP TABLE IF EXISTS t2;
    """

    sql """
    CREATE TABLE `t2` (

    `logTimestamp` datetime NULL,

    `args1` varchar(65533) NULL,

    `args2` variant NULL,

    `args3` int NULL,

    `args4` text NULL,

    `args5` varchar(65533) NULL,

    `args6` varchar(65533) NULL,

    `args7` varchar(200) NULL,

    `args8` varchar(65533) NULL,

    `args9` variant NULL,

    `args10` variant NULL,

    `args11` variant NULL,

    `args12` variant NULL,

    `args13` varchar(65533) NULL,

    `args14` text NULL,

    `args15` variant NULL,

    `log` variant NULL

    ) ENGINE=OLAP

    DUPLICATE KEY(`logTimestamp`)

    DISTRIBUTED BY RANDOM BUCKETS 10

    PROPERTIES (

            "replication_num" = "1"
    );
    """

    sql """
    INSERT INTO t2 (
            `logTimestamp`, `args1`, `args2`, `args3`, `args4`,
            `args5`, `args6`, `args7`, `args8`, `args9`,
            `args10`, `args11`, `args12`, `args13`, `args14`,
            `args15`, `log`
    ) VALUES

    ('2025-07-31 10:15:30', 'user_id=123&action=login', '{"type":"authentication","status":"success"}', 200, 'User login successful',
    '192.168.1.100', '/api/v1/login', '192.168.1.100', 'api.example.com', '{"app":"web","env":"production"}',
    '{"country":"China","city":"Beijing"}', '{"Content-Type":"application/json","Authorization":"Bearer abc123"}', '{"hostname":"web-server-01"}', 'https://referer.example.com', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    '{"input_type":"http","tags":["auth","prod"]}', '{"timestamp":"2025-07-31T10:15:30Z","level":"info","message":"login success"}'),


    ('2025-07-31 11:30:45', 'product_id=456&category=electronics', '{"type":"product_view","device":"mobile"}', 304, 'Product page viewed',
    '192.168.1.101', '/products/456', '192.168.1.101', 'shop.example.com', '{"app":"ecommerce","env":"staging"}',
    '{"country":"USA","city":"New York"}', '{"Content-Type":"text/html","Cache-Control":"max-age=3600"}', '{"hostname":"shop-server-02"}', 'https://search.example.com', 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X)',
    '{"input_type":"http","tags":["shop","mobile"]}', '{"timestamp":"2025-07-31T11:30:45Z","level":"info","message":"product view"}'),


    ('2025-07-31 14:45:20', 'order_id=789&amount=99.99', '{"type":"payment","method":"credit_card"}', 201, 'Payment processed',
    '192.168.1.102', '/api/v1/payments', '192.168.1.102', 'pay.example.com', '{"app":"payment","env":"production"}',
    '{"country":"UK","city":"London"}', '{"Content-Type":"application/json","X-CSRF-Token":"xyz789"}', '{"hostname":"payment-gateway-01"}', 'https://checkout.example.com', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    '{"input_type":"api","tags":["payment","prod"]}', '{"timestamp":"2025-07-31T14:45:20Z","level":"info","message":"payment success"}'),


    ('2025-07-31 16:30:10', 'search_term=laptop&page=2', '{"type":"search","results_count":25}', 200, 'Search results returned',
    '192.168.1.103', '/search', '192.168.1.103', 'search.example.com', '{"app":"search","env":"production"}',
    '{"country":"Japan","city":"Tokyo"}', '{"Content-Type":"application/json","Accept-Language":"en-US"}', '{"hostname":"search-server-03"}', 'https://www.example.com', 'Mozilla/5.0 (Linux; Android 11; SM-G991B)',
    '{"input_type":"search","tags":["search","prod"]}', '{"timestamp":"2025-07-31T16:30:10Z","level":"info","message":"search query"}'),


    ('2025-07-31 18:15:55', 'error_code=500&message=Internal+Server+Error', '{"type":"error","severity":"high"}', 500, 'Internal server error occurred',
    '192.168.1.104', '/api/v1/users', '192.168.1.104', 'api.example.com', '{"app":"web","env":"production"}',
    '{"country":"Germany","city":"Berlin"}', '{"Content-Type":"application/json","X-Request-ID":"req123"}', '{"hostname":"web-server-02"}', 'https://client.example.com', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    '{"input_type":"error","tags":["error","prod"]}', '{"timestamp":"2025-07-31T18:15:55Z","level":"error","message":"server error"}');
     """


    sql "DROP VIEW IF EXISTS view_test"


    sql """
    CREATE VIEW `view_test`AS
    SELECT   t1.`logTimestamp`,
    t1.`args1`,
    t1.`args2`,
    t1.`args3`,
    t1.`args4`,
    t1.`args5`,
    t1.`args6`,
    t1.`args7`,
    t1.`args8`,
    t1.`args9`,
    t1.`args10`,
    t1.`args11`,
    t1.`args12`,
    t1.`args13`,
    t1.`args14`,
    t1.`args15`,
    t1.`log`
    FROM t1
    UNION all
    SELECT   t2.`logTimestamp`,
    t2.`args1`,
    t2.`args2`,
    t2.`args3`,
    t2.`args4`,
    t2.`args5`,
    t2.`args6`,
    t2.`args7`,
    t2.`args8`,
    t2.`args9`,
    t2.`args10`,
    t2.`args11`,
    t2.`args12`,
    t2.`args13`,
    t2.`args14`,
    t2.`args15`,
    t2.`log`
    FROM t2;
    """


    order_qt_union_all_push_down_top_n """
    SELECT `args1`, `args2`, `args3`, `args4`, `args5`, `args6`, `args7`, `args8`, `args9`, `args10`, `args11`, `args12`, `args13`, `args14`, `args15`, `log`, logTimestamp
    FROM view_test
    ORDER BY logTimestamp desc
    LIMIT 8;"""


    qt_union_all_push_down_top_n_shape """
    explain shape plan SELECT `args1`, `args2`, `args3`, `args4`, `args5`, `args6`, `args7`, `args8`, `args9`, `args10`, `args11`, `args12`, `args13`, `args14`, `args15`, `log`, logTimestamp
    FROM view_test
    ORDER BY logTimestamp desc
    LIMIT 8;
    """
}