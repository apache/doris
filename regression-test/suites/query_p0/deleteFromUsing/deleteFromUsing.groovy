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

suite("deleteFromUsing") {
    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            unique key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"


    sql "set dry_run_query=true;"

    //  fix DELETE/UPDATE cannot resolve column when table alias uses AS keyword                                                                                   
                                                                                                                                                                           
    //   When using 'DELETE FROM table AS t1', the tableAlias was incorrectly                                                                                                     
    //   set to 'AS t1' instead of 't1', causing column resolution to fail                                                                                                        
    //   in subqueries. Changed to use strictIdentifier().getText() to get                                                                                                        
    //   only the identifier part.
    sql """
        delete from d_table as t1
        using d_table as t2
        where t2.k2 = 2
          and t1.k1 = t2.k1;
    """
}
