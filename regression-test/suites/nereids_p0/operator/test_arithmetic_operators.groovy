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


suite("test_arithmetic_operators", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "test"

    sql "use test_query_db"

    qt_arith_op1 "select k1, k4 div k1, k4 div k2, k4 div k3, k4 div k4 \
		    from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op2 "select k1, k1+ '1', k5,100000*k5 from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op3 "select k1,k5,k2*k5 from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op4 "select k1,k5,k8*k5,k5*k9,k2*k9,k2*k8 from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op5 "select k1, k5*0.1, k8*0.1, k9*0.1 from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op6 "select k1, k2*(-0.1), k3*(-0.1), k4*(-0.1), \
		    k5*(-0.1), k8*(-0.1), k9*(-0.1) from  ${tableName} order by k1, k2, k3, k4"
    qt_arith_op7 "select k1, k5*(9223372036854775807/100), k8*9223372036854775807, \
		    k9*9223372036854775807 from  ${tableName} order by k1, k2, k3, k4"
    qt_arith_op8 "select k1, k2/9223372036854775807, k3/9223372036854775807, \
		    k4/9223372036854775807,k5/9223372036854775807, \
		    k8/9223372036854775807,k9/9223372036854775807 \
		    from  ${tableName} order by k1, k2, k3, k4"
    qt_arith_op9 "select k1, k5+9223372036854775807/100, k8+9223372036854775807, \
		    k9+9223372036854775807 from  ${tableName} order by k1, k2, k3, k4"
    qt_arith_op10 "select k1, k5-9223372036854775807/100, k8-9223372036854775807, \
		    k9-9223372036854775807 from  ${tableName} order by k1, k2, k3, k4"
    qt_arith_op11 "select k1, k5/0.000001, k8/0.000001, \
		    k9/0.000001 from  ${tableName} order by k1, k2, k3, k4"
    qt_arith_op12 "select k1, k1*0.1, k2*0.1, k3*0.1, k4*0.1, k5*0.1, k8*0.1, k9*0.1 \
		    from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op13 "select k1, k1/10, k2/10, k3/10, k4/10, k5/10, k8/10, k9/10 \
		    from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op14 "select k1, k1-0.1, k2-0.1, k3-0.1, k4-0.1, k5-0.1, k8-0.1, k9-0.1 \
		    from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op15 "select k1, k1+0.1, k2+0.1, k3+0.1, k4+0.1, k5+0.1, k8+0.1, k9+0.1 \
		    from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op16 "select k1+10, k2+10.0, k3+1.6, k4*1, k5-6, k8-234.66, k9-0 \
		    from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op17 "select * from ${tableName} where k1+k9<0 order by k1, k2, k3, k4"
    qt_arith_op18 "select k1*k2*k3*k5 from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op19 "select k1*k2*k3*k5*k8*k9 from ${tableName} order by k1, k2, k3, k4"
    qt_arith_op20 "select k1*10000/k4/k8/k9 from ${tableName} order by k1, k2, k3, k4"
    
    for( i in [1, 2, 3, 5, 8, 9]) {
        for( j in [1, 2, 3, 5, 8, 9]) {
            qt_arith_op21 "select k${i}*k${j}, k${i}+k${j}, k${i}-k${j}, k${i}/k${j} from ${tableName} \
			    where abs(k${i})<9223372036854775807 and k${j}<>0 and\
			    abs(k${i})<922337203685477580 order by k1, k2, k3, k4"
        }
    }
    
    qt_arith_op22 "select 1.1*1.1 + k2 from ${tableName} order by 1 limit 10"
    qt_arith_op23 "select 1.1*1.1 + k5 from ${tableName} order by 1 limit 10"
    qt_arith_op24 "select 1.1*1.1+1.1"

    // divide mod zero
    qt_arith_op25 "select 10.2 / 0.0, 10.2 / 0, 10.2 % 0.0, 10.2 % 0"
    qt_arith_op26 "select 0.0 / 0.0, 0.0 / 0, 0.0 % 0.0, 0.0 % 0"
    qt_arith_op27 "select -10.2 / 0.0, -10.2 / 0, -10.2 % 0.0, -10.2 % 0"
    qt_arith_op28 "select k5 / 0, k8 / 0, k9 / 0 from ${tableName} order by k1,k2,k3,k4"
    qt_arith_op29 "select k5 % 0, k8 % 0, k9 % 0 from ${tableName} order by k1,k2,k3,k4"
}
