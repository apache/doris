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

suite("explain_alias") {
    sql """
    create table t (
        a int,
        b int,
        c int
    ) distributed by hash(a)
    properties (
        'replication_num' = '1'
    );

    insert into t values (1, 10, 100), (2, 20, 200), (3, 30, 300);

    """

    explain {
        sql """
            verbose 
            select sum(a) as mysum from t;
        """
        contains "col=mysum"
    }
        /**
        even SlotDescriptor.column is null, the col in explain is not null
        | Tuples:                                                                                                                                                        |
        | TupleDescriptor{id=0, tbl=t}                                                                                                                                   |
        |   SlotDescriptor{id=0, col=a, colUniqueId=0, type=int, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}                              |
        |                                                                                                                                                                |
        | TupleDescriptor{id=1, tbl=t}                                                                                                                                   |
        |   SlotDescriptor{id=3, col=a, colUniqueId=0, type=int, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}                              |
        |                                                                                                                                                                |
        | TupleDescriptor{id=2, tbl=null}                                                                                                                                |
        |   SlotDescriptor{id=4, col=partial_sum(a)#4, colUniqueId=null, type=varchar(65533), nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null} |
        |                                                                                                                                                                |
        | TupleDescriptor{id=3, tbl=null}                                                                                                                                |
        |   SlotDescriptor{id=5, col=mysum#3, colUniqueId=null, type=bigint, nullable=true, isAutoIncrement=false, subColPath=null, virtualColumn=null}         
        */

}
