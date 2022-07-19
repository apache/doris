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

import org.apache.doris.catalog.Catalog;
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
                        Catalog.class.getDeclaredMethod("loadMasterInfo", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveMasterInfo", CountingDataOutputStream.class, long.class);
                break;
            case "frontends":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadFrontends", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveFrontends", CountingDataOutputStream.class, long.class);
                break;
            case "backends":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadBackends", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveBackends", CountingDataOutputStream.class, long.class);
                break;
            case "db":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadDb", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveDb", CountingDataOutputStream.class, long.class);
                break;
            case "loadJob":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadLoadJob", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveLoadJob", CountingDataOutputStream.class, long.class);
                break;
            case "alterJob":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadAlterJob", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveAlterJob", CountingDataOutputStream.class, long.class);
                break;
            case "recycleBin":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadRecycleBin", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveRecycleBin", CountingDataOutputStream.class, long.class);
                break;
            case "globalVariable":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadGlobalVariable", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveGlobalVariable", CountingDataOutputStream.class,
                                long.class);
                break;
            case "cluster":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadCluster", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveCluster", CountingDataOutputStream.class, long.class);
                break;
            case "broker":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadBrokers", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveBrokers", CountingDataOutputStream.class, long.class);
                break;
            case "resources":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadResources", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveResources", CountingDataOutputStream.class, long.class);
                break;
            case "exportJob":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadExportJob", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveExportJob", CountingDataOutputStream.class, long.class);
                break;
            case "syncJob":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadSyncJobs", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveSyncJobs", CountingDataOutputStream.class, long.class);
                break;
            case "backupHandler":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadBackupHandler", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveBackupHandler",
                                CountingDataOutputStream.class, long.class);
                break;
            case "paloAuth":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadPaloAuth", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("savePaloAuth", CountingDataOutputStream.class, long.class);
                break;
            case "transactionState":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadTransactionState", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveTransactionState", CountingDataOutputStream.class,
                                long.class);
                break;
            case "colocateTableIndex":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadColocateTableIndex", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveColocateTableIndex", CountingDataOutputStream.class,
                                long.class);
                break;
            case "routineLoadJobs":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadRoutineLoadJobs", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveRoutineLoadJobs", CountingDataOutputStream.class,
                                long.class);
                break;
            case "loadJobV2":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadLoadJobsV2", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveLoadJobsV2", CountingDataOutputStream.class, long.class);
                break;
            case "smallFiles":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadSmallFiles", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveSmallFiles", CountingDataOutputStream.class, long.class);
                break;
            case "plugins":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadPlugins", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("savePlugins", CountingDataOutputStream.class, long.class);
                break;
            case "deleteHandler":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadDeleteHandler", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveDeleteHandler",
                                CountingDataOutputStream.class, long.class);
                break;
            case "sqlBlockRule":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadSqlBlockRule", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveSqlBlockRule", CountingDataOutputStream.class, long.class);
                break;
            case "policy":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadPolicy", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("savePolicy", CountingDataOutputStream.class, long.class);
                break;
            case "datasource":
                metaPersistMethod.readMethod =
                        Catalog.class.getDeclaredMethod("loadDatasource", DataInputStream.class, long.class);
                metaPersistMethod.writeMethod =
                        Catalog.class.getDeclaredMethod("saveDatasource", CountingDataOutputStream.class, long.class);
                break;
            case "autoBatchLoadTableAndBe":
                metaPersistMethod.readMethod = Catalog.class.getDeclaredMethod("loadAutoBatchLoadTableAndBe",
                        DataInputStream.class, long.class);
                metaPersistMethod.writeMethod = Catalog.class.getDeclaredMethod("saveAutoBatchLoadTableAndBe",
                        CountingDataOutputStream.class, long.class);
                break;
            default:
                break;
        }
        return metaPersistMethod;
    }
}
