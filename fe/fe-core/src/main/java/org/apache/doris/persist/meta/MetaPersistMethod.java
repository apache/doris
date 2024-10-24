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

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.io.CountingDataOutputStream;

import java.io.DataInputStream;
import java.lang.reflect.Method;

/**
 * Defines a write and read method for the metadata module
 * that needs to be persisted to the image.
 */
public class MetaPersistMethod {
    public String name;
    public Method readMethod;
    public Method writeMethod;

    public MetaPersistMethod(String name) {
        this.name = name;
    }

    /**
     * All meta modules should be added to this method.
     * Modules' names are defined in {@link PersistMetaModules}
     *
     * @param name
     * @return
     * @throws NoSuchMethodException
     */
    public static MetaPersistMethod create(String name) throws NoSuchMethodException {
        MetaPersistMethod metaPersistMethod = new MetaPersistMethod(name);
        switch (name) {
            case "masterInfo":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadMasterInfo", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveMasterInfo", CountingDataOutputStream.class, long.class);
                break;
            case "frontends":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadFrontends", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveFrontends", CountingDataOutputStream.class, long.class);
                break;
            case "backends":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadBackends", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveBackends", CountingDataOutputStream.class, long.class);
                break;
            case "db":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadDb", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveDb", CountingDataOutputStream.class, long.class);
                break;
            case "alterJob":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadAlterJob", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveAlterJob", CountingDataOutputStream.class, long.class);
                break;
            case "cloudWarmUpJob":
                metaPersistMethod.readMethod = CloudEnv.class.getDeclaredMethod(
                        "loadCloudWarmUpJob", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod = CloudEnv.class.getDeclaredMethod(
                        "saveCloudWarmUpJob", CountingDataOutputStream.class, long.class);
                break;
            case "recycleBin":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadRecycleBin", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveRecycleBin", CountingDataOutputStream.class, long.class);
                break;
            case "globalVariable":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadGlobalVariable", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveGlobalVariable", CountingDataOutputStream.class,
                                long.class);
                break;
            case "cluster":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadCluster", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveCluster", CountingDataOutputStream.class, long.class);
                break;
            case "broker":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadBrokers", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveBrokers", CountingDataOutputStream.class, long.class);
                break;
            case "resources":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadResources", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveResources", CountingDataOutputStream.class, long.class);
                break;
            case "exportJob":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadExportJob", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveExportJob", CountingDataOutputStream.class, long.class);
                break;
            case "syncJob":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadSyncJobs", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveSyncJobs", CountingDataOutputStream.class, long.class);
                break;
            case "backupHandler":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadBackupHandler", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveBackupHandler",
                                CountingDataOutputStream.class, long.class);
                break;
            case "paloAuth":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadAuth", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveAuth", CountingDataOutputStream.class, long.class);
                break;
            case "transactionState":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadTransactionState", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveTransactionState", CountingDataOutputStream.class,
                                long.class);
                break;
            case "colocateTableIndex":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadColocateTableIndex", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveColocateTableIndex", CountingDataOutputStream.class,
                                long.class);
                break;
            case "routineLoadJobs":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadRoutineLoadJobs", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveRoutineLoadJobs", CountingDataOutputStream.class,
                                long.class);
                break;
            case "loadJobV2":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadLoadJobsV2", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveLoadJobsV2", CountingDataOutputStream.class, long.class);
                break;
            case "smallFiles":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadSmallFiles", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveSmallFiles", CountingDataOutputStream.class, long.class);
                break;
            case "plugins":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadPlugins", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("savePlugins", CountingDataOutputStream.class, long.class);
                break;
            case "deleteHandler":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadDeleteHandler", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveDeleteHandler",
                                CountingDataOutputStream.class, long.class);
                break;
            case "sqlBlockRule":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadSqlBlockRule", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveSqlBlockRule", CountingDataOutputStream.class, long.class);
                break;
            case "policy":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadPolicy", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("savePolicy", CountingDataOutputStream.class, long.class);
                break;
            case "datasource":
                metaPersistMethod.readMethod = Env.class.getDeclaredMethod("loadCatalog", DataInputStream.class,
                        long.class);
                metaPersistMethod.writeMethod = Env.class.getDeclaredMethod("saveCatalog",
                        CountingDataOutputStream.class, long.class);
                break;
            case "globalFunction":
                metaPersistMethod.readMethod = Env.class.getDeclaredMethod("loadGlobalFunction", DataInputStream.class,
                        long.class);
                metaPersistMethod.writeMethod = Env.class.getDeclaredMethod("saveGlobalFunction",
                        CountingDataOutputStream.class, long.class);
                break;
            case "workloadGroups":
            case "resourceGroups":
                // In 2.0 alpha, there are many people using resource groups, maybe not upgrade from 2.0 alpha.
                // So that add a compatible code here.
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadWorkloadGroups", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveWorkloadGroups", CountingDataOutputStream.class, long.class);
                break;
            case "workloadSchedPolicy":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadWorkloadSchedPolicy", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveWorkloadSchedPolicy", CountingDataOutputStream.class,
                                long.class);
                break;
            case "binlogs":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadBinlogs", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveBinlogs", CountingDataOutputStream.class, long.class);
                break;
            case "AnalysisMgr":
            case "AnalysisMgrV2":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadAnalysisManager", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveAnalysisMgr", CountingDataOutputStream.class, long.class);
                break;
            case "AsyncJobManager":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadAsyncJobManager", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveAsyncJobManager", CountingDataOutputStream.class, long.class);
                break;
            case "JobTaskManager":
                break;
            case "insertOverwrite":
                metaPersistMethod.readMethod =
                        Env.class.getDeclaredMethod("loadInsertOverwrite", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Env.class.getDeclaredMethod("saveInsertOverwrite", CountingDataOutputStream.class, long.class);
                break;
            case "plsql":
                // package and stored procedure use the same method in PlsqlManager.
                metaPersistMethod.readMethod = Env.class.getDeclaredMethod("loadPlsqlProcedure", DataInputStream.class,
                        long.class);
                metaPersistMethod.writeMethod = Env.class.getDeclaredMethod("savePlsqlProcedure",
                        CountingDataOutputStream.class, long.class);
                break;
            default:
                break;
        }
        return metaPersistMethod;
    }
}
