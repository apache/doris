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

suite("nereids_unnest_fn") {
    sql 'use regression_test_nereids_function_p0'

    order_qt_sql_unnest_bitmap '''
        select id, e from fn_test, unnest(to_bitmap(kbint)) as tmp(e) order by id, e'''
    order_qt_sql_unnest_bitmap_notnull '''
        select id, e from fn_test_not_nullable, unnest(to_bitmap(kbint)) as tmp(e) order by id, e'''

    order_qt_sql_unnest_outer_bitmap '''
        select id, e from fn_test left outer join unnest(to_bitmap(kbint)) as tmp(e) on true order by id, e'''
    order_qt_sql_unnest_outer_bitmap_notnull '''
        select id, e from fn_test_not_nullable left outer join unnest(to_bitmap(kbint)) as tmp(e) on true order by id, e'''

    order_qt_sql_unnest_array "select id, e from fn_test, unnest(kaint) as tmp(e) order by id, e"
    order_qt_sql_unnest_array_notnull "select id, e from fn_test_not_nullable, unnest(kaint) as tmp(e) order by id, e"
    order_qt_sql_unnest_outer_array '''
        select id, e from fn_test left outer join unnest(kaint) as tmp(e) on true order by id, e'''
    order_qt_sql_unnest_outer_array_notnull '''
        select id, e from fn_test_not_nullable left outer join unnest(kaint) as tmp(e) on true order by id, e'''

    order_qt_sql_unnest_map "select id, e, x from fn_test, unnest(km_int_int) as tmp(e, x) order by id, e"
    order_qt_sql_unnest_map_notnull "select id, e, x from fn_test_not_nullable, unnest(km_int_int) as tmp(e, x) order by id, e"
    order_qt_sql_unnest_outer_map '''
        select id, e, x from fn_test left outer join unnest(km_int_int) as tmp(e, x) on true order by id, e'''
    order_qt_sql_unnest_outer_map_notnull '''
        select id, e, x from fn_test_not_nullable left outer join unnest(km_int_int) as tmp(e, x) on true order by id, e'''

    order_qt_sql_unnest_literal_from_1 '''SELECT * FROM unnest([1,2,3]);'''
    order_qt_sql_unnest_literal_from_2 '''SELECT * FROM unnest([1,2,NULL,3], ['11',NULL,'22']);'''
    order_qt_sql_unnest_literal_from_nested '''SELECT t.* FROM unnest([[1,2],[3],NULL,[4,5,6]]) AS t;'''
    order_qt_sql_unnest_literal_from_hybrid '''SELECT t.* FROM unnest([[1,2],[3],NULL,[4,5,6]], ['hi','hello']) AS t(c1,c2);'''
    order_qt_sql_unnest_literal_from_multiple '''SELECT * FROM unnest([1,2,3]) t1(c1), unnest(['11','22']) AS t2(c2);'''

    order_qt_sql_unnest_literal_select_1 '''SELECT unnest([1,2,3]);'''
    order_qt_sql_unnest_literal_select_nested '''SELECT unnest([[1,2],[3],NULL,[4,5,6]]) AS t;'''
    order_qt_sql_unnest_literal_select_multiple '''SELECT unnest([1,2,3]) AS c1, unnest(['11','22']) AS c2;'''

    multi_sql '''
        DROP TABLE IF EXISTS sales_unnest_t;
        DROP TABLE IF EXISTS products_dict_unnest_t;
        DROP TABLE IF EXISTS items_dict_unnest_t;
        DROP TABLE IF EXISTS categories_dict_unnest_t;
        DROP TABLE IF EXISTS student_unnest_t;

        CREATE TABLE sales_unnest_t (
            sale_id INT,
            region VARCHAR(50),
            products ARRAY<INT>, 
            amount DECIMAL(10, 2),
            sale_date DATE
        ) ENGINE=OLAP
        DUPLICATE KEY(sale_id)
        DISTRIBUTED BY HASH(sale_id) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );

        CREATE TABLE products_dict_unnest_t (
            product_id INT,
            product_name VARCHAR(50)
        ) ENGINE=OLAP
        DUPLICATE KEY(product_id)
        DISTRIBUTED BY HASH(product_id) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );

        CREATE TABLE items_dict_unnest_t (
            id INT,
            name VARCHAR(50),
            tags ARRAY<VARCHAR(50)>, 
            price DECIMAL(10,2),
            category_ids ARRAY<INT>  
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );

        CREATE TABLE categories_dict_unnest_t (
            id INT KEY,
            name VARCHAR(30) 
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );

        CREATE TABLE student_unnest_t
        (
        `id` bigint(20) NULL COMMENT "",
        `scores` ARRAY<int> NULL COMMENT ""
        )
        DUPLICATE KEY (id)
        DISTRIBUTED BY HASH(`id`)
        PROPERTIES (
        "replication_num" = "1"
        );

        INSERT INTO items_dict_unnest_t (id, name, tags, price, category_ids) VALUES
        (1, 'Laptop', ['Electronics', 'Office', 'High-End', 'Laptop'], 5999.99, [1, 2, 3]),
        (2, 'Mechanical Keyboard', ['Electronics', 'Accessories'], 399.99, [1, 2]),
        (3, 'Basketball', ['Sports', 'Outdoor'], 199.99, [1,3]),
        (4, 'Badminton Racket', ['Sports', 'Equipment'], 299.99, [3]),
        (5, 'Shirt', ['Clothing', 'Office', 'Shirt'], 259.00, [4]);

        INSERT INTO categories_dict_unnest_t (id, name) VALUES
        (1, 'Digital Products'),
        (2, 'Office Supplies'),
        (3, 'Sports Equipment'),
        (4, 'Apparel');

        INSERT INTO sales_unnest_t (sale_id, region, products, amount, sale_date) VALUES
        (1, 'North', [101, 102], 1500.00, '2023-01-15'),
        (2, 'North', [101], 900.00, '2023-01-20'),
        (3, 'South', [102, 103], 2500.00, '2023-01-15'),
        (4, 'South', [103], 1200.00, '2023-01-25'),
        (5, 'East', [101, 103], 1800.00, '2023-01-18'),
        (6, 'West', [102], 800.00, '2023-01-22');

        INSERT INTO products_dict_unnest_t (product_id, product_name) VALUES
        (101, 'Laptop'),
        (102, 'Mouse'),
        (103, 'Keyboard');

        INSERT INTO student_unnest_t VALUES
        (1, [80,85,87]),
        (2, [77, null, 89]),
        (3, null),
        (4, []),
        (5, [90,92]);
    '''

    order_qt_sql_grouping_set '''
    SELECT
        region,
        p.product_name,
        COUNT(s.sale_id) AS order_count,
        SUM(s.amount) AS total_sales
    FROM
        sales_unnest_t s
    CROSS JOIN unnest(s.products) AS p_id
    LEFT JOIN products_dict_unnest_t p ON p_id = p.product_id
    GROUP BY
        GROUPING SETS (
            (unnest(s.products)),
            (region),                
            (p.product_name),        
            (region, p.product_name)
        )
    ORDER BY
        region, p.product_name;
    '''

    order_qt_sql_cube '''
    SELECT
        region,
        p.product_name,
        EXTRACT(YEAR FROM s.sale_date) AS sale_year,
        COUNT(s.sale_id) AS order_count,
        SUM(s.amount) AS total_sales
    FROM
        sales_unnest_t s
    CROSS JOIN unnest(s.products) AS p_id
    LEFT JOIN products_dict_unnest_t p ON p_id = p.product_id
    GROUP BY
        CUBE (
            (unnest(s.products)),
            region,
            p.product_name,
            EXTRACT(YEAR FROM s.sale_date)
        )
    ORDER BY
        region, p.product_name, sale_year;
    '''

    order_qt_sql_select '''
    SELECT
        id,
        name,
        unnest(tags) AS tag,
        price
    FROM
        items_dict_unnest_t
    ORDER BY
        id;
    '''

    order_qt_sql_from '''
    SELECT
        i.id,
        i.name,
        t.tag,
        t.ord
    FROM
        items_dict_unnest_t i,
        unnest(i.tags) with ordinality AS t(tag, ord)
    ORDER BY
        i.id,
        i.name;
    '''

    order_qt_sql_cross_join '''
    SELECT
        i.id,
        i.name,
        c.name AS category_name
    FROM
        items_dict_unnest_t i
        CROSS JOIN unnest(i.category_ids) AS cid
        INNER JOIN categories_dict_unnest_t c ON cid = c.id
    ORDER BY
        i.id,
        i.name;
    '''

    order_qt_sql_left_join '''
    SELECT
        id,
        scores,
        unnest
    FROM
        student_unnest_t
        LEFT JOIN unnest(scores) AS unnest ON TRUE
    ORDER BY
        1,
        3;
    '''

    order_qt_sql_group_by '''
    SELECT
        unnest(i.tags),
        COUNT(DISTINCT i.id) AS item_count,
        AVG(i.price) AS avg_price
    FROM
        items_dict_unnest_t i
    GROUP BY
        unnest(i.tags)
    ORDER BY
        unnest(i.tags);
    '''

    order_qt_sql_order_by '''
    SELECT 
        id, 
        name, 
        unnest(tags),
        price
    FROM items_dict_unnest_t
    ORDER BY 
        unnest(tags), 
        price DESC;  
    '''

    order_qt_sql_window '''
    SELECT
        id,
        name,
        unnest(tags) AS tag,
        price,
        RANK() OVER (
            PARTITION BY unnest(tags)
            ORDER BY
                price DESC
        ) AS price_rank
    FROM
        items_dict_unnest_t
    ORDER BY
        id,
        name;
    '''

    order_qt_sql_distinct '''
    SELECT
        DISTINCT unnest(tags) AS unique_tag
    FROM
        items_dict_unnest_t
    ORDER BY
        unnest(tags);
    '''

    order_qt_sql_count '''
    select
        name,
        count(unnest(tags))
    from
        items_dict_unnest_t
    group by
        name
    having
        count(unnest(tags)) = 2
    order by
        name;
    '''

    order_qt_sql_sum '''
    select
        name,
        sum(unnest(category_ids))
    from
        items_dict_unnest_t
    group by
        name
    having
        sum(unnest(category_ids)) = 3
    order by
        name;
    '''

    order_qt_sql_count_window '''
    select
        name,
        tags,
        unnest(tags),
        count(unnest(tags)) over(partition by tags)
    from
        items_dict_unnest_t
    group by
        name, tags, unnest(tags)
    order by
        name, tags, unnest(tags);
    '''

    order_qt_sql_sum_window '''
    select
        name,
        category_ids,
        unnest(category_ids),
        sum(unnest(category_ids)) over(partition by category_ids)
    from
        items_dict_unnest_t
    group by
        name, category_ids, unnest(category_ids)
    order by
        name, category_ids, unnest(category_ids);
    '''

    order_qt_sql_sum_window2 '''
    select
        name,
        category_ids,
        unnest(category_ids),
        sum(unnest(category_ids)) over(partition by name)
    from
        items_dict_unnest_t
    group by
        name, category_ids, unnest(category_ids)
    order by
        name, category_ids, unnest(category_ids);
    '''

    order_qt_sql_inner_join0 '''
    SELECT
        id,
        name,
        tags,
        t.tag
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_inner_join1 '''
    SELECT
        id,
        name,
        tags,
        t.tag
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON name in ('Laptop', 'Basketball');
    '''
    order_qt_sql_inner_join2 '''
    SELECT
        id,
        name,
        tags,
        t.tag
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON tag in ('Electronics', 'Sports');
    '''
    order_qt_sql_inner_join3 '''
    SELECT
        id
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_inner_join4 '''
    SELECT
        name
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_inner_join5 '''
    SELECT
        tags
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_inner_join6 '''
    SELECT
        t.tag
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_inner_join7 '''
    SELECT
        id,
        t.tag
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_inner_join8 '''
    SELECT
        1
    FROM
        items_dict_unnest_t
        INNER JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''

    order_qt_sql_left_join0 '''
    SELECT
        id,
        name,
        tags,
        t.tag
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_left_join1 '''
    SELECT
        id,
        name,
        tags,
        t.tag
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON name in ('Laptop', 'Basketball');
    '''
    order_qt_sql_left_join2 '''
    SELECT
        id,
        name,
        tags,
        t.tag
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON tag in ('Electronics', 'Sports');
    '''

    order_qt_sql_left_join3 '''
    SELECT
        id
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_left_join4 '''
    SELECT
        name
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_left_join5 '''
    SELECT
        tags
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_left_join6 '''
    SELECT
        t.tag
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_left_join7 '''
    SELECT
        id,
        t.tag
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''
    order_qt_sql_left_join8 '''
    SELECT
        1
    FROM
        items_dict_unnest_t
        LEFT JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
    '''

    test {
        sql '''
        SELECT
            id,
            name,
            tags,
            t.tag
        FROM
            items_dict_unnest_t
            right JOIN lateral unnest(tags) AS t(tag) ON t.tag = name;
        '''
        exception "must be INNER, LEFT or CROSS"
    }

    // test big array
    sql 'use regression_test_nereids_function_p0_gen_function'
    sql """
    set batch_size = 10;
    """
    multi_sql '''
    DROP TABLE if exists big_array_unnest_t;
    CREATE TABLE big_array_unnest_t (
        id INT,
        tags ARRAY<INT>
    ) properties (
        "replication_num" = "1"
    );
    '''
    // one child row unnest to batch_size or more output rows
    sql '''
    insert into big_array_unnest_t values
    (1, [1,2,1,4,5,6,7,8,9,10]),
    (2, [1,3,3,4,5,6,7,8,9,10]),
    (3, [1,2]),
    (4, [4,4]),
    (5, null),
    (6, [1,null,2,null,5]),
    (7, [1, 7]),
    (8, [1, 9]),
    (9, [1,2,3,4,5,6,  1,2,3,4,5,6,7,8,9,9]),
    (10, [1,2,3,4,5,6,  1,2,3,4,5,6,7,8,9,10]);
    '''
    qt_big_array_unnest_all '''
    select id, array_size(tags), array_sort(tags) from big_array_unnest_t order by id;
    '''

    qt_big_array_unnest0 '''
    select id, array_size(tags), array_sort(tags), unnest(tags) as tag from big_array_unnest_t order by id, tag;
    '''

    qt_big_array_unnest_inner0 '''
    select id, t.tag from big_array_unnest_t INNER JOIN lateral unnest(tags) AS t(tag) ON tag = id order by id, tag;
    '''

    qt_big_array_unnest_outer0 '''
    select id, t.tag from big_array_unnest_t LEFT JOIN lateral unnest(tags) AS t(tag) ON tag = id order by id, tag;
    '''

    sql """
    truncate table big_array_unnest_t;
    """
    sql '''
    insert into big_array_unnest_t values
    (1, [1,2,3]),
    (2, [3,4,5,6,7,8,9,10,11,12,13,14]),
    (3, [3,3,4,5,6,7,8,9,10,11,12,13,14,15]);
    '''
    qt_big_array_unnest_all_1 '''
    select id, array_size(tags), array_sort(tags) from big_array_unnest_t order by id;
    '''

    qt_big_array_unnest1 '''
    select id, array_size(tags), array_sort(tags), unnest(tags) as tag from big_array_unnest_t order by id, tag;
    '''

    qt_big_array_unnest_inner1 '''
    select id, t.tag from big_array_unnest_t INNER JOIN lateral unnest(tags) AS t(tag) ON tag = id order by id, tag;
    '''

    qt_big_array_unnest_outer1 '''
    select id, t.tag from big_array_unnest_t LEFT JOIN lateral unnest(tags) AS t(tag) ON tag = id order by id, tag;
    '''

    sql """
    truncate table big_array_unnest_t;
    """
    sql '''
    insert into big_array_unnest_t values
    (1, [1, 2, 3,4,5,6,7,8,9,10]),
    (2, [1,3,4]),
    (3, [3,3,4,5,6,7,8,9,10,11,12,13,14,15]);
    '''
    qt_big_array_unnest_all_2 '''
    select id, array_size(tags), array_sort(tags) from big_array_unnest_t order by id;
    '''

    qt_big_array_unnest2 '''
    select id, array_size(tags), array_sort(tags), unnest(tags) as tag from big_array_unnest_t order by id, tag;
    '''

    qt_big_array_unnest_inner2 '''
    select id, t.tag from big_array_unnest_t INNER JOIN lateral unnest(tags) AS t(tag) ON tag = id order by id, tag;
    '''

    qt_big_array_unnest_outer2 '''
    select id, t.tag from big_array_unnest_t LEFT JOIN lateral unnest(tags) AS t(tag) ON tag = id order by id, tag;
    '''

    sql """
    truncate table big_array_unnest_t;
    """
    sql '''
    insert into big_array_unnest_t values
    (1, [1, 2, 3]),
    (2, [1,3,4]),
    (3, [3,3,4,5,6,7,8,9,10,11,12,13,14,15]),
    (4, []),
    (5, null),
    (6, [1,null,2, 6]);
    '''
    qt_big_array_unnest_all_3 '''
    select id, array_size(tags), array_sort(tags) from big_array_unnest_t order by id;
    '''

    qt_big_array_unnest3 '''
    select id, array_size(tags), array_sort(tags), unnest(tags) as tag from big_array_unnest_t order by id, tag;
    '''

    qt_big_array_unnest_inner3 '''
    select id, t.tag from big_array_unnest_t INNER JOIN lateral unnest(tags) AS t(tag) ON tag = id order by id, tag;
    '''

    qt_big_array_unnest_outer3 '''
    select id, t.tag from big_array_unnest_t LEFT JOIN lateral unnest(tags) AS t(tag) ON tag = id order by id, tag;
    '''

    qt_full_join_unnest_literal '''
    select * from unnest([1,2,3]) t1(c1) full join unnest(['11','22']) AS t2(c2) on t1.c1 = t2.c2 order by 1, 2;
    '''

}
