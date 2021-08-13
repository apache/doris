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

package org.apache.doris.planner;

public class DistributedPlanColocateRule {
    public static final String SESSION_DISABLED = "Session disabled";
    public static final String HAS_JOIN_HINT = "Has join hint";
    public static final String TRANSFORMED_SRC_COLUMN = "Src column hash been transformed by expr";
    public static final String REDISTRIBUTED_SRC_DATA = "The src data has been redistributed";
    public static final String SUPPORT_ONLY_OLAP_TABLE = "Only olap table support colocate plan";
    public static final String TABLE_NOT_IN_THE_SAME_GROUP = "Tables are not in the same group";
    public static final String COLOCATE_GROUP_IS_NOT_STABLE = "Colocate group is not stable";
    public static final String INCONSISTENT_DISTRIBUTION_OF_TABLE_AND_QUERY = "Inconsistent distribution of table and querie";
}
