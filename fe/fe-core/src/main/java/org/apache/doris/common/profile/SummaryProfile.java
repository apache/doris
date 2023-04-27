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

package org.apache.doris.common.profile;

import org.apache.doris.common.Version;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.ProfileManager;

import com.google.common.collect.Maps;

import java.util.Map;

public class SummaryProfile {
    public static final String JOB_ID = "Job ID";
    public static final String QUERY_ID = "Query ID";
    public static final String QUERY_TYPE = "Query Type";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String QUERY_STATE = "Query State";
    public static final String DORIS_VERSION = "Doris Version";
    public static final String USER = "User";
    public static final String DEFAULT_DB = "Default Db";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String IS_CACHED = "Is Cached";

    public static final String TOTAL_INSTANCES_NUM = "Total Instances Num";

    public static final String INSTANCES_NUM_PER_BE = "Instances Num Per BE";

    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE = "Parallel Fragment Exec Instance Num";


    Map<String, String> infos = Maps.newLinkedHashMap();
        infos.put(ProfileManager.JOB_ID, "N/A");
        infos.put(ProfileManager.QUERY_ID, DebugUtil.printId(context.queryId()));
        infos.put(ProfileManager.QUERY_TYPE, queryType);
        infos.put(ProfileManager.DORIS_VERSION, Version.DORIS_BUILD_VERSION);
        infos.put(ProfileManager.USER, context.getQualifiedUser());
        infos.put(ProfileManager.DEFAULT_DB, context.getDatabase());
        infos.put(ProfileManager.SQL_STATEMENT, originStmt.originStmt);
        infos.put(ProfileManager.IS_CACHED, isCached ? "Yes" : "No");

    Map<String, Integer> beToInstancesNum =
            coord == null ? Maps.newTreeMap() : coord.getBeToInstancesNum();
        infos.put(ProfileManager.TOTAL_INSTANCES_NUM,
            String.valueOf(beToInstancesNum.values().stream().reduce(0, Integer::sum)));
        infos.put(ProfileManager.INSTANCES_NUM_PER_BE, beToInstancesNum.toString());
        infos.put(ProfileManager.PARALLEL_FRAGMENT_EXEC_INSTANCE,
            String.valueOf(context.sessionVariable.parallelExecInstanceNum));
        return infos;
}
