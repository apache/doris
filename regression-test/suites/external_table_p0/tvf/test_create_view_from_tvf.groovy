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

 suite("test_create_view_from_tvf","p0,external,tvf,external_docker") {
    String testViewName = "test_view_from_number"

    def create_view = {createViewSql -> 
       sql """ drop view if exists ${testViewName} """
       sql createViewSql
    }

    def sql1 =  """ CREATE VIEW ${testViewName}
                    (
                        k1 COMMENT "number"
                    )
                    COMMENT "my first view"
                    AS
                    SELECT number as k1 FROM numbers("number" = "10");
                """
    create_view(sql1)
    order_qt_view1 """ select * from ${testViewName} """


    def sql2 =  """ CREATE VIEW ${testViewName}
                    AS
                    SELECT * FROM numbers("number" = "10");
                """
    create_view(sql2)
    order_qt_view2 """ select * from ${testViewName} """

    def sql_count =  """ CREATE VIEW ${testViewName}
                        AS
                        select count(*) from numbers("number" = "100");
                     """
    create_view(sql_count)
    order_qt_count """ select * from ${testViewName} """

    def sql_inner_join =  """ CREATE VIEW ${testViewName}
                            AS
                            select a.number as num1, b.number as num2
                            from numbers("number" = "10") a inner join numbers("number" = "10") b 
                            on a.number=b.number;
                        """
    create_view(sql_inner_join)
    order_qt_inner_join """ select * from ${testViewName} """

    def sql_left_join = """ CREATE VIEW ${testViewName}
                            AS
                            select a.number as num1, b.number as num2
                            from numbers("number" = "10") a left join numbers("number" = "5") b 
                            on a.number=b.number order by num1;
                        """
    create_view(sql_left_join)
    order_qt_left_join """ select * from ${testViewName} """

    def sql_where_equal = """ CREATE VIEW ${testViewName}
                              AS
                              select * from numbers("number" = "10") where number%2 = 1;
                          """
    create_view(sql_where_equal)
    order_qt_where_equal """ select * from ${testViewName} """

    def sql_where_lt = """ CREATE VIEW ${testViewName}
                           AS
                           select * from numbers("number" = "10") where number+1 < 9;
                       """
    create_view(sql_where_lt)
    order_qt_where_lt """ select * from ${testViewName} """

    def sql_join_where  = """ CREATE VIEW ${testViewName}
                           AS
                           select a.number as num1, b.number as num2
                           from numbers("number" = "10") a inner join numbers("number" = "10") b 
                           on a.number=b.number where a.number>4;
                       """
    create_view(sql_join_where)
    order_qt_join_where """ select * from ${testViewName} """

    def sql_groupby  = """ CREATE VIEW ${testViewName}
                            AS
                            select number from numbers("number" = "10") where number>=4 group by number order by number;
                       """
    create_view(sql_groupby)
    order_qt_groupby """ select * from ${testViewName} """

    def sql_subquery1  = """ CREATE VIEW ${testViewName}
                            AS
                            select * from numbers("number" = "10") where number = (select number from numbers("number" = "10") where number=1);
                       """
    create_view(sql_subquery1)
    order_qt_subquery1 """ select * from ${testViewName} """
     
 }
 