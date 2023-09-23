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
// This file is copied from
// https://github.com/klout/brickhouse/blob/master/src/main/java/brickhouse/udf/collect/GroupCountUDF.java
// and modified by Doris

package org.apache.doris.udf.collect;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


/**
 * GroupCountUDF provides a sequence number for all rows which have the
 * same value for a particular grouping.
 * This allows us to count how many rows are in a grouping and cap them
 * off after a certain point.
 * <p/>
 * <p>For example, we can cap-off the number of records per ks_uid with something like
 * <p/>
 * select
 * ks_uid, val, group_count(ks_uid) as rank
 * from
 * (  select ks_uid, val from table1
 * distribute  by ks_uid
 * sort by ks_uid, val ) ordered_keys
 * where group_count( ks_uid ) < 100
 */
@Description(
    name = "group_count",
    value = " A sequence id for all rows with the same value for a specific grouping"
)
public class GroupCountUDF extends UDF {
    private String lastGrouping = null;
    private int lastCount = 0;

    public Integer evaluate(String grouping) {
        // First time through ...
        if (lastGrouping == null) {
            lastGrouping = grouping;
            lastCount = 1;
            return 0;
        }
        if (lastGrouping != null
            && lastGrouping.equals(grouping)) {
            int retVal = lastCount;
            lastCount++;
            return retVal;
        } else {
            lastCount = 1;
            lastGrouping = grouping;
            return 0;
        }

    }
}
