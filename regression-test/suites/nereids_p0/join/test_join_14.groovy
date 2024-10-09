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

suite("test_join_14", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql 'set parallel_fragment_exec_instance_num = 2;'
    sql "use nereids_test_query_db"

    def tbName1 = "test"
    def tbName2 = "baseall"
    def tbName3 = "bigtable"
    def empty_name = "empty"

    List selected = ["a.k1, b.k1, a.k2, b.k2, a.k3, b.k3", "count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)"]

    // join with no join keyword
    for (s in selected){
        qt_join_without_keyword1"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where a.k1 = b.k1 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword2"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where a.k1 > b.k1 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword3"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword4"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where a.k1 = b.k1 and a.k2 > 0 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword5"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where a.k1 = b.k1 and a.k2 > b.k2 and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword6"""select ${s} from ${tbName1} a , ${tbName2} b 
               where a.k2 > 0 and b.k3 != 0 and a.k6 > "000" order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword7"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where (a.k1 = b.k1 or a.k2 = b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword8"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where (a.k1 < b.k1 or a.k2 > b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword9"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where (a.k1 = b.k1 or a.k2 > b.k2) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword10"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where (a.k1 = b.k1 or a.k2 > 0) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword11"""select ${s} from ${tbName1} a , ${tbName2} b 
                    where (a.k1 < b.k1 or a.k2 > 0) and a.k2 > 0 and b.k3 != 0 and a.k6 > "000" 
                    order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword12"""select ${s} from ${tbName1} a, ${tbName2} b, ${tbName3} c where a.k1 = b.k1 
                and a.k2 = c.k2 order by 1, 2, 3, 4, 5 limit 65535"""
        qt_join_without_keyword13"""select ${s} from ${tbName1} a, ${tbName2} b, ${tbName3} c where a.k2 = b.k2 and a.k1 > 0 
                and a.k3 = c.k3 and b.k1 = c.k1 + 1 and c.k3 > 0 
                order by 1, 2, 3, 4, 5 limit 65535"""
    }

    // join with empty table
    qt_join_with_emptyTable1"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName2} a join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    qt_join_with_emptyTable2"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName2} a inner join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    qt_join_with_emptyTable3"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName2} a left join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    qt_join_with_emptyTable4"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName2} a right join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    def res53 = sql"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName2} a full outer join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    def res54 = sql"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName2} a left join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3, 4, 5"""
    check2_doris(res53, res54)
    // qt_join_with_emptyTable5"""select a.k1, a.k2, a.k3, b.k1, b.k2, b.k3 from ${tbName2} a cross join ${empty_name} b on a.k1 = b.k1
    //         order by 1, 2, 3, 4, 5"""
    test {
        sql"""select a.k1, a.k2, a.k3 from ${tbName2} a left semi join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3"""
        check{result, exception, startTime, endTime ->
            logger.info(result.toString())
            assertTrue(result.isEmpty())
        }
    }
    test {
        sql"""select b.k1, b.k2, b.k3 from ${tbName2} a right semi join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }
    def res55 = sql"""select a.k1, a.k2, a.k3 from ${tbName2} a left anti join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3"""
    def res56 = sql"""select k1, k2, k3 from ${tbName2} order by 1, 2, 3"""
    check2_doris(res55, res56)
    test {
        sql"""select b.k1, b.k2, b.k3 from ${tbName2} a right anti join ${empty_name} b on a.k1 = b.k1 
            order by 1, 2, 3"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }



    // cases for bug
    def res57 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
           right semi join test b on a.k1 = b.k1 and a.k2 > b.k2 order by 1, 2, 3, 4 limit 65535"""
    def res58 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
           right semi join test b on a.k1 = b.k1 and a.k2 < b.k2 order by 1, 2, 3, 4 limit 65535"""
    assertTrue(res57 == res58)


    def res59 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
           right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535"""
    def res60 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
          right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535"""
    for (j in range(0, 100)) {
        def res61 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
                   right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535"""
        def res62 = sql"""select count(b.k1), count(b.k2), count(b.k4), count(*) from baseall a 
                  right semi join test b on a.k1 = b.k1 order by 1, 2, 3, 4 limit 65535"""
        check2_doris(res61, res59)
        check2_doris(res62, res60)
    }


    def res63 = sql"""select count(*) from test a full outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
            full outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and a.k3 > 0 
            order by 1 limit 65535"""
    def res64 = sql"""select count(*) from test a full outer join baseall b on a.k2 = b.k2 and a.k1 > 0 
           full outer join bigtable c on a.k3 = c.k3 and b.k1 = c.k1 and c.k3 > 0 
           order by 1 limit 65535"""
    check2_doris(res63, res64)

    sql"drop view if exists nullable_14"
    sql"""create view nullable_14(n1, n2) as select a.k1, b.k2 from baseall 
            a left join bigtable b on a.k1 = b.k1 + 10 where b.k2 is null"""
    qt_join_bug1"""select k1, n1 from baseall a right outer join nullable_14 b on a.k1 % 2 = b.n1 % 2 
           order by a.k1, b.n1"""
    qt_join_bug2"""select n.k1, m.k1, m.k2, n.k2 from (select a.k1, a.k2, a.k3 from 
           baseall a join baseall b on (a.k1 = b.k1 and a.k2 = b.k2 and a.k3 = b.k3)) m 
           left join test n on m.k1 = n.k1 order by 1, 2, 3, 4"""
    // https://github.com/apache/doris/issues/4210
    qt_join_bug3"""select * from baseall t1 where k1 = (select min(k1) from test t2 where t2.k1 = t1.k1 and t2.k2=t1.k2)
           order by k1"""
    qt_join_bug4"""select b.k1 from baseall b where b.k1 not in( select k1 from baseall where k1 is not null )"""


    // basic join
    List columns = ["k1", "k2", "k3", "k4", "k5", "k6", "k10", "k11"]
    List join_types = ["inner", "left outer", "right outer", ""]
    for (type in join_types) {
        for (c in columns) {
            qt_join_basic1"""select * from ${tbName2} a ${type} join ${tbName1} b on (a.${c} = b.${c}) 
                   order by isnull(a.k1), a.k1, a.k2, a.k3, isnull(b.k1), b.k1, b.k2, b.k3 
                   limit 60015"""
        }
    }
    for (c in columns){
        sql"""select * from ${tbName2} a full outer join ${tbName1} b on (a.${c} = b.${c}) 
                order by isnull(a.k1), a.k1, a.k2, a.k3, a.k4, isnull(b.k1), b.k1, b.k2, b.k3, 
                b.k4 limit 65535"""
        sql"""select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak7, a.k10 ak10, a.k11 ak11, 
                 a.k7 ak7, a.k8 ak8, a.k9 ak9, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, b.k5 bk5, 
                 b.k6 bk6, b.k10 bk10, b.k11 bk11, b.k7 bk7, b.k8 bk8, b.k9 bk9 
                 from ${tbName2} a left outer join ${tbName1} b on (a.${c} = b.${c}) 
                 union select a.k1 ak1, a.k2 ak2, a.k3 ak3, a.k4 ak4, a.k5 ak5, a.k6 ak6, a.k10 ak10, 
                 a.k11 ak11, a.k7 ak7, a.k8 ak8, a.k9 ak9, b.k1 bk1, b.k2 bk2, b.k3 bk3, b.k4 bk4, 
                 b.k5 bk5, b.k6 bk6, b.k10 bk10, b.k11 bk11, b.k7 bk7, b.k8 bk8, b.k9 bk9 from 
                 ${tbName2} a right outer join ${tbName1} b on (a.${c} = b.${c}) order by 
                 isnull(ak1), 1, 2, 3, 4, isnull(bk1), 12, 13, 14, 15 limit 65535"""

        def res67 = sql"""select * from ${tbName2} a left semi join ${tbName1} b on (a.${c} = b.${c}) 
                order by a.k1, a.k2, a.k3"""
        def res68 = sql"""select distinct a.* from ${tbName2} a left outer join ${tbName1} b on (a.${c} = b.${c}) 
                where b.k1 is not null order by a.k1, a.k2, a.k3"""
        check2_doris(res67, res68)

        def res69 = sql"""select * from ${tbName2} a right semi join ${tbName1} b on (a.${c} = b.${c}) 
                order by b.k1, b.k2, b.k3"""
        def res70 = sql"""select distinct b.* from ${tbName2} a right outer join ${tbName1} b on (a.${c} = b.${c}) 
                where a.k1 is not null order by b.k1, b.k2, b.k3"""
        check2_doris(res69, res70)

        def res71 = sql"""select * from ${tbName2} a left anti join ${tbName1} b on (a.${c} = b.${c}) 
                order by a.k1, a.k2, a.k3"""
        def res72 = sql"""select distinct a.* from ${tbName2} a left outer join ${tbName1} b on (a.${c} = b.${c}) 
                where b.k1 is null order by a.k1, a.k2, a.k3"""
        check2_doris(res71, res72)

        def res73 = sql"""select * from ${tbName2} a right anti join ${tbName1} b on (a.${c} = b.${c}) 
                order by b.k1, b.k2, b.k3"""
        def res74 = sql"""select distinct b.* from ${tbName2} a right outer join ${tbName1} b on (a.${c} = b.${c}) 
                where a.k1 is null order by b.k1, b.k2, b.k3"""
        check2_doris(res73, res74)
    }
}