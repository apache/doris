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

suite("test_explain_tpch_sf_1_q12", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  l_shipmode,
		  sum(CASE
		      WHEN o_orderpriority = '1-URGENT'
		           OR o_orderpriority = '2-HIGH'
		        THEN 1
		      ELSE 0
		      END) AS high_line_count,
		  sum(CASE
		      WHEN o_orderpriority <> '1-URGENT'
		           AND o_orderpriority <> '2-HIGH'
		        THEN 1
		      ELSE 0
		      END) AS low_line_count
		FROM
		  orders,
		  lineitem
		WHERE
		  o_orderkey = l_orderkey
		  AND l_shipmode IN ('MAIL', 'SHIP')
		  AND l_commitdate < l_receiptdate
		  AND l_shipdate < l_commitdate
		  AND l_receiptdate >= DATE '1994-01-01'
		  AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
		GROUP BY
		  l_shipmode
		ORDER BY
		  l_shipmode

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 10> <slot 7> `l_shipmode` ASC") && 
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(CASE WHEN ((<slot 21> = '1-URGENT' OR <slot 21> = '2-HIGH') AND (<slot 21> = '1-URGENT' OR <slot 21> = '2-HIGH')) THEN 1 ELSE 0 END), sum(CASE WHEN <slot 21> != '1-URGENT' AND <slot 21> != '2-HIGH' THEN 1 ELSE 0 END)\n" + 
				"  |  group by: <slot 15>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: `l_orderkey` = `o_orderkey`\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `o_orderkey`") && 
		explainStr.contains("vec output tuple id: 4") && 
		explainStr.contains("output slot ids: 15 21 \n" + 
				"  |  hash output slot ids: 0 1 ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. agg expr [FunctionCallExpr{name=sum, isStar=false, isDistinct=false, ((((SlotRef{slotDesc=SlotDescriptor{id=1, parent=0, col=O_ORDERPRIORITY, type=CHAR(15), materialized=true, byteSize=0, byteOffset=-1, nullIndicatorByte=0, nullIndicatorBit=0, slotIdx=0}, col=o_orderpriority, label=`o_orderpriority`, tblName=null} ) (SlotRef{slotDesc=SlotDescriptor{id=1, parent=0, col=O_ORDERPRIORITY, type=CHAR(15), materialized=true, byteSize=0, byteOffset=-1, nullIndicatorByte=0, nullIndicatorBit=0, slotIdx=0}, col=o_orderpriority, label=`o_orderpriority`, tblName=null} ))  ))}] is not bound [, `default_cluster:regression_test_tpch_sf1_p1`.`lineitem`]\n" + 
				"     PREDICATES: `l_shipmode` IN ('MAIL', 'SHIP'), `l_commitdate` < `l_receiptdate`, `l_shipdate` < `l_commitdate`, `l_receiptdate` >= '1994-01-01 00:00:00', `l_receiptdate` < '1995-01-01 00:00:00', `default_cluster:regression_test_tpch_sf1_p1.lineitem`.`__DORIS_DELETE_SIGN__` = 0\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `l_orderkey`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.orders(orders), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts.\n" + 
				"     PREDICATES: `default_cluster:regression_test_tpch_sf1_p1.orders`.`__DORIS_DELETE_SIGN__` = 0") 
            
        }
    }
}