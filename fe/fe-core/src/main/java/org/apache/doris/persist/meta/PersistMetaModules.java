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

package org.apache.doris.persist.meta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Save all MetaPersistMethods.
 */
public class PersistMetaModules {
    // module name -> MetaPersistMethod
    public static final Map<String, MetaPersistMethod> MODULES_MAP;
    // Save MetaPersistMethod in order.
    // The write and read of meta modules should be in same order.
    public static final List<MetaPersistMethod> MODULES_IN_ORDER;

    public static final ImmutableList<String> MODULE_NAMES = ImmutableList.of(
            "masterInfo", "frontends", "backends", "datasource", "db", "alterJob", "recycleBin",
            "globalVariable", "cluster", "broker", "resources", "exportJob", "syncJob", "backupHandler",
            "paloAuth", "transactionState", "colocateTableIndex", "routineLoadJobs", "loadJobV2", "smallFiles",
            "plugins", "deleteHandler", "sqlBlockRule", "policy", "mtmvJobManager", "globalFunction", "workloadGroups",
            "binlogs", "resourceGroups", "AnalysisMgrV2", "AsyncJobManager", "JobTaskManager");

    // Modules in this list is deprecated and will not be saved in meta file. (also should not be in MODULE_NAMES)
    public static final ImmutableList<String> DEPRECATED_MODULE_NAMES = ImmutableList.of(
            "loadJob", "cooldownJob", "AnalysisMgr");

    static {
        MODULES_MAP = Maps.newHashMap();
        MODULES_IN_ORDER = Lists.newArrayList();
        try {
            for (String name : MODULE_NAMES) {
                MetaPersistMethod persistMethod = MetaPersistMethod.create(name);
                MODULES_MAP.put(name, persistMethod);
                MODULES_IN_ORDER.add(persistMethod);
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
