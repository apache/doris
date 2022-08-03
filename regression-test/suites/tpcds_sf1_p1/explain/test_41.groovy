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

suite("test_regression_test_tpcds_sf1_p1_q41", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT DISTINCT i_product_name
		FROM
		  item i1
		WHERE (i_manufact_id BETWEEN 738 AND (738 + 40))
		   AND ((
		      SELECT count(*) item_cnt
		      FROM
		        item
		      WHERE ((i_manufact = i1.i_manufact)
		            AND (((i_category = 'Women')
		                  AND ((i_color = 'powder')
		                     OR (i_color = 'khaki'))
		                  AND ((i_units = 'Ounce')
		                     OR (i_units = 'Oz'))
		                  AND ((i_size = 'medium')
		                     OR (i_size = 'extra large')))
		               OR ((i_category = 'Women')
		                  AND ((i_color = 'brown')
		                     OR (i_color = 'honeydew'))
		                  AND ((i_units = 'Bunch')
		                     OR (i_units = 'Ton'))
		                  AND ((i_size = 'N/A')
		                     OR (i_size = 'small')))
		               OR ((i_category = 'Men')
		                  AND ((i_color = 'floral')
		                     OR (i_color = 'deep'))
		                  AND ((i_units = 'N/A')
		                     OR (i_units = 'Dozen'))
		                  AND ((i_size = 'petite')
		                     OR (i_size = 'large')))
		               OR ((i_category = 'Men')
		                  AND ((i_color = 'light')
		                     OR (i_color = 'cornflower'))
		                  AND ((i_units = 'Box')
		                     OR (i_units = 'Pound'))
		                  AND ((i_size = 'medium')
		                     OR (i_size = 'extra large')))))
		         OR ((i_manufact = i1.i_manufact)
		            AND (((i_category = 'Women')
		                  AND ((i_color = 'midnight')
		                     OR (i_color = 'snow'))
		                  AND ((i_units = 'Pallet')
		                     OR (i_units = 'Gross'))
		                  AND ((i_size = 'medium')
		                     OR (i_size = 'extra large')))
		               OR ((i_category = 'Women')
		                  AND ((i_color = 'cyan')
		                     OR (i_color = 'papaya'))
		                  AND ((i_units = 'Cup')
		                     OR (i_units = 'Dram'))
		                  AND ((i_size = 'N/A')
		                     OR (i_size = 'small')))
		               OR ((i_category = 'Men')
		                  AND ((i_color = 'orange')
		                     OR (i_color = 'frosted'))
		                  AND ((i_units = 'Each')
		                     OR (i_units = 'Tbl'))
		                  AND ((i_size = 'petite')
		                     OR (i_size = 'large')))
		               OR ((i_category = 'Men')
		                  AND ((i_color = 'forest')
		                     OR (i_color = 'ghost'))
		                  AND ((i_units = 'Lb')
		                     OR (i_units = 'Bundle'))
		                  AND ((i_size = 'medium')
		                     OR (i_size = 'extra large')))))
		   ) > 0)
		ORDER BY i_product_name ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 13> <slot 12> `i_product_name` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 12> `i_product_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 15>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (`i1`.`i_manufact` = <slot 5> `i_manufact`)\n" + 
				"  |  other predicates: (ifnull(<slot 23>, 0) > 0)") && 
		explainStr.contains("other predicates: (ifnull(<slot 23>, 0) > 0)") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 15 \n" + 
				"  |  hash output slot ids: 6 10 ") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `i_manufact_id` >= 738, `i_manufact_id` <= 778") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 6> count(*))\n" + 
				"  |  group by: <slot 5> `i_manufact`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: `i_manufact`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((((`i_category` = 'Women') AND ((`i_color` = 'powder') OR (`i_color` = 'khaki')) AND ((`i_units` = 'Ounce') OR (`i_units` = 'Oz')) AND ((`i_size` = 'medium') OR (`i_size` = 'extra large'))) OR ((`i_category` = 'Women') AND ((`i_color` = 'brown') OR (`i_color` = 'honeydew')) AND ((`i_units` = 'Bunch') OR (`i_units` = 'Ton')) AND ((`i_size` = 'N/A') OR (`i_size` = 'small'))) OR ((`i_category` = 'Men') AND ((`i_color` = 'floral') OR (`i_color` = 'deep')) AND ((`i_units` = 'N/A') OR (`i_units` = 'Dozen')) AND ((`i_size` = 'petite') OR (`i_size` = 'large'))) OR ((`i_category` = 'Men') AND ((`i_color` = 'light') OR (`i_color` = 'cornflower')) AND ((`i_units` = 'Box') OR (`i_units` = 'Pound')) AND ((`i_size` = 'medium') OR (`i_size` = 'extra large')))) OR (((`i_category` = 'Women') AND ((`i_color` = 'midnight') OR (`i_color` = 'snow')) AND ((`i_units` = 'Pallet') OR (`i_units` = 'Gross')) AND ((`i_size` = 'medium') OR (`i_size` = 'extra large'))) OR ((`i_category` = 'Women') AND ((`i_color` = 'cyan') OR (`i_color` = 'papaya')) AND ((`i_units` = 'Cup') OR (`i_units` = 'Dram')) AND ((`i_size` = 'N/A') OR (`i_size` = 'small'))) OR ((`i_category` = 'Men') AND ((`i_color` = 'orange') OR (`i_color` = 'frosted')) AND ((`i_units` = 'Each') OR (`i_units` = 'Tbl')) AND ((`i_size` = 'petite') OR (`i_size` = 'large'))) OR ((`i_category` = 'Men') AND ((`i_color` = 'forest') OR (`i_color` = 'ghost')) AND ((`i_units` = 'Lb') OR (`i_units` = 'Bundle')) AND ((`i_size` = 'medium') OR (`i_size` = 'extra large')))))") 
            
        }
    }
}