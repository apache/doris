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


 suite("test_numbers","p0,external,external_docker") {
    // Test basic features
    order_qt_basic1 """ select * from numbers("number" = "1"); """
    order_qt_basic2 """ select * from numbers("number" = "10"); """
    order_qt_basic3 """ select * from numbers("number" = "100"); """
    order_qt_basic4_limit """ select * from numbers("number" = "10") limit 5; """

    order_qt_const1 """ select * from numbers("number" = "5", "const_value" = "1"); """
    order_qt_const2 """ select * from numbers("number" = "5", "const_value" = "-123"); """
    order_qt_const3 """ select * from numbers("number" = "-10", "const_value" = "1"); """
    order_qt_const4 """ select avg(number) from numbers("number" = "100", "const_value" = "123"); """

    // Test aggregate function withh numbers("number" = N)
    order_qt_agg_sum """ select sum(number) from numbers("number" = "100"); """
    order_qt_agg_avg """ select avg(number) from numbers("number" = "100"); """
    order_qt_agg_count """ select count(*) from numbers("number" = "100"); """
    order_qt_agg_min """ select min(number) from numbers("number" = "100"); """
    order_qt_agg_max """ select max(number) from numbers("number" = "100"); """

    // Test join with numbers("number" = N)
    order_qt_inner_join1 """
                    select a.number as num1, b.number as num2
                    from numbers("number" = "10") a inner join numbers("number" = "10") b 
                    on a.number=b.number ORDER BY a.number,b.number;
                  """
    order_qt_inner_join2 """
                    select a.number as num1, b.number as num2
                    from numbers("number" = "6") a inner join numbers("number" = "6") b
                    on a.number>b.number ORDER BY a.number,b.number;
                  """
    order_qt_inner_join3 """
                    select a.number as num1, b.number as num2
                    from numbers("number" = "10") a inner join numbers("number" = "10") b
                    on a.number=b.number and b.number%2 = 0 ORDER BY a.number,b.number;
                  """
    order_qt_left_join """
                    select a.number as num1, b.number as num2
                    from numbers("number" = "10") a left join numbers("number" = "5") b 
                    on a.number=b.number order by num1;
                  """
    order_qt_right_join """
                    select a.number as num1, b.number as num2
                    from numbers("number" = "5") a right join numbers("number" = "10") b 
                    on a.number=b.number order by num2;
                  """
    
    // Test where and GroupBy
    order_qt_where_equal """ select * from numbers("number" = "10") where number%2 = 1; """
    order_qt_where_gt """ select * from numbers("number" = "10") where number-1 > 1; """
    order_qt_where_lt """ select * from numbers("number" = "10") where number+1 < 9; """
    order_qt_groupby """ select number from numbers("number" = "10") where number>=4 group by number order by number; """
    order_qt_join_where """
                    select a.number as num1, b.number as num2
                    from numbers("number" = "10") a inner join numbers("number" = "10") b 
                    on a.number=b.number where a.number>4 order by num1,num2;
                  """
    
    // Test Sub Query
    order_qt_subquery1 """ select * from numbers("number" = "10") where number = (select number from numbers("number" = "10") where number=1); """
    order_qt_subquery2 """ select * from numbers("number" = "10") where number in (select number from numbers("number" = "10") where number>5); """
    order_qt_subquery3 """ select a.number from numbers("number" = "10") a where number in (select number from numbers("number" = "10") b where a.number=b.number); """
    
    // Test window function
    order_qt_window_1 """ SELECT row_number() OVER (ORDER BY number) AS id,number from numbers("number" = "10"); """
    order_qt_window_2 """ SELECT number, rank() OVER (order by number) AS sum_three from numbers("number" = "10"); """
    order_qt_window_3 """ SELECT number, dense_rank() OVER (order by number) AS sum_three from numbers("number" = "10"); """
    order_qt_window_4 """ SELECT number, sum(number) OVER (ORDER BY number rows between 1 preceding and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_5 """ SELECT number, min(number) OVER (ORDER BY number rows between 1 PRECEDING and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_6 """ SELECT number, min(number) OVER (ORDER BY number rows between UNBOUNDED PRECEDING and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_7 """ SELECT number, max(number) OVER (ORDER BY number rows between 1 preceding and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_8 """ SELECT number, max(number) OVER (ORDER BY number rows between UNBOUNDED PRECEDING and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_9 """ SELECT number, avg(number) OVER (ORDER BY number rows between 1 preceding and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_10 """ SELECT number, count(number) OVER (ORDER BY number rows between 1 preceding and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_11 """ SELECT number, first_value(number) OVER (ORDER BY number rows between 1 preceding and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_12 """ SELECT number, last_value(number) OVER (ORDER BY number rows between 1 preceding and 1 following) AS result from numbers("number" = "10"); """
    order_qt_window_13 """ SELECT number, LAG(number,2,-1) OVER (ORDER BY number) AS result from numbers("number" = "10"); """

    // Cast BITINT to STRING and test string function.
    order_qt_stringfunction_1 """ select cast (number as string) as string_num from numbers("number" = "10"); """
    order_qt_stringfunction_2 """ select append_trailing_char_if_absent(cast (number as string),'a') as string_fucntion_res from numbers("number" = "10"); """
    order_qt_stringfunction_3 """ select concat(cast (number as string),'abc','d') as string_fucntion_res from numbers("number" = "10"); """
    order_qt_stringfunction_4 """ select concat(cast (number as string), cast (number as string)) as string_fucntion_res from numbers("number" = "10"); """
    order_qt_stringfunction_5 """ select ascii(cast (number as string)) as string_fucntion_res from numbers("number" = "12"); """
    order_qt_stringfunction_6 """ select bit_length(cast (number as string)) as string_fucntion_res from numbers("number" = "14") where number>5; """
    order_qt_stringfunction_7 """ select char_length(cast (number as string)) as string_fucntion_res from numbers("number" = "14") where number>5; """
    order_qt_stringfunction_8 """ select concat_ws('-',cast (number as string),'a') as string_fucntion_res from numbers("number" = "14") where number>5; """
    order_qt_stringfunction_9 """ select number, ends_with(cast (number as string),'1') as string_fucntion_res from numbers("number" = "12"); """
    order_qt_stringfunction_10 """ select number,find_in_set(cast (number as string),'0,1,2,3,4,5,6,7') as string_fucntion_res from numbers("number" = "10"); """
    order_qt_stringfunction_11 """ select number,hex(number) as string_fucntion_res from numbers("number" = "13") where number>5; """
    order_qt_stringfunction_12 """ select number,hex(cast (number as string)) as string_fucntion_res from numbers("number" = "13") where number>5; """
    order_qt_stringfunction_13 """ select number,instr(cast (number as string),'1') as string_fucntion_res from numbers("number" = "13") where number>5; """
    order_qt_stringfunction_14 """ select number,left(cast (number as string),'2') as string_fucntion_res from numbers("number" = "1000") where number>120 limit 10; """
    order_qt_stringfunction_15 """ select number,length(cast (number as string)) as string_fucntion_res from numbers("number" = "1000") where number>120 limit 10; """
    order_qt_stringfunction_16 """ select number,locate('2',cast (number as string)) as string_fucntion_res from numbers("number" = "1000") where number>120 limit 10; """
    order_qt_stringfunction_17 """ select number,locate('2',cast (number as string),3) as string_fucntion_res from numbers("number" = "1000") where number>120 limit 10; """
    order_qt_stringfunction_18 """ select number,lpad(cast (number as string),3,'0') as string_fucntion_res from numbers("number" = "1000") where number>95 limit 15; """
    order_qt_stringfunction_19 """ select ltrim( concat('  a',cast (number as string))) as string_fucntion_res from numbers("number" = "10"); """
    order_qt_stringfunction_20 """ select repeat(cast (number as string),2) as string_fucntion_res from numbers("number" = "13"); """
    order_qt_stringfunction_21 """ select replace(cast (number as string),'1','a') as string_fucntion_res from numbers("number" = "13"); """
    order_qt_stringfunction_22 """ select reverse(cast (number as string)) as string_fucntion_res from numbers("number" = "20") where number>9; """
    order_qt_stringfunction_23 """ select right(cast (number as string),1) as string_fucntion_res from numbers("number" = "20") where number>9; """
    order_qt_stringfunction_24 """ select number,rpad(cast (number as string),3,'0') as string_fucntion_res from numbers("number" = "1000") where number>95 limit 15; """
    order_qt_stringfunction_25 """ select STARTS_WITH(cast (number as string),'1') as string_fucntion_res from numbers("number" = "15"); """
    order_qt_stringfunction_26 """ select strleft(cast (number as string),'2') as string_fucntion_res from numbers("number" = "200") where number>105 limit 10; """
    order_qt_stringfunction_27 """ select strright(cast (number as string),'2') as string_fucntion_res from numbers("number" = "1000") where number>105 limit 10; """
    order_qt_stringfunction_28 """ select substring(cast (number as string),2) as string_fucntion_res from numbers("number" = "1000") where number>105 limit 10; """
    order_qt_stringfunction_29 """ select substring(cast (number as string),-1) as string_fucntion_res from numbers("number" = "1000") where number>105 limit 10; """
    order_qt_stringfunction_30 """ select number,unhex(cast (number as string)) as string_fucntion_res from numbers("number" = "100") limit 30; """

    // test subquery
    order_qt_subquery_1 """ with a as (select number from numbers("number"="3")) select * from a; """
    order_qt_subquery_2 """ select * from (select number from numbers("number"="3")) a join (select * from (select number from numbers("number"="1")) a join (select 1) b) b; """

    // test exception
    test {
        sql """ select * from numbers('number' = 'abc'); """

        // check exception
        exception "cannot parse param value abc"
    }

    test {
        sql """ select * from numbers(); """

        // check exception
        exception """number not set"""
    }
 }
