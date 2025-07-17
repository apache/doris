/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
suite('initial_join_order') {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    if (isCloudMode()) {
        return
    }
    sql "use ${db}"
    sql """
        set disable_nereids_rules=PRUNE_EMPTY_PARTITION;
        set runtime_filter_mode=off;
        set memo_max_group_expression_size=1;
        """

    // check the initial join order: lineitem join region, not region join lineitem
    qt_shape """
        explain shape plan
        select count()
        from region 
            left join lineitem on r_regionkey= l_orderkey
            join nation on r_regionkey=n_nationkey
            join supplier on n_nationkey=s_nationkey
            join customer on r_regionkey=c_custkey 
    """
}