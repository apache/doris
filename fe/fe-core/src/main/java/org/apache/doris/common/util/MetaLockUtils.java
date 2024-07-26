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

package org.apache.doris.common.util;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.MetaNotFoundException;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * MetaLockUtils is a helper class to lock and unlock all meta object in a list.
 * In order to escape dead lock, meta object in list should be sorted in ascending
 * order by id first, and then MetaLockUtils can lock them.
 */
public class MetaLockUtils {

    public static void readLockDatabases(List<? extends DatabaseIf> databaseList) {
        for (DatabaseIf database : databaseList) {
            database.readLock();
        }
    }

    public static void readUnlockDatabases(List<? extends DatabaseIf> databaseList) {
        for (int i = databaseList.size() - 1; i >= 0; i--) {
            databaseList.get(i).readUnlock();
        }
    }

    public static void readLockTables(List<? extends TableIf> tableList) {
        for (TableIf table : tableList) {
            table.readLock();
        }
    }

    public static void readUnlockTables(List<? extends TableIf> tableList) {
        for (int i = tableList.size() - 1; i >= 0; i--) {
            tableList.get(i).readUnlock();
        }
    }

    public static void writeLockTables(List<? extends TableIf> tableList) {
        for (TableIf table : tableList) {
            table.writeLock();
        }
    }

    public static List<? extends TableIf> writeLockTablesIfExist(List<? extends TableIf> tableList) {
        List<TableIf> lockedTablesList = Lists.newArrayListWithCapacity(tableList.size());
        for (TableIf table : tableList) {
            if (table.writeLockIfExist()) {
                lockedTablesList.add(table);
            }
        }
        return lockedTablesList;
    }

    public static boolean tryWriteLockTablesIfExist(List<? extends TableIf> tableList, long timeout,
            TimeUnit unit) {
        for (int i = 0; i < tableList.size(); i++) {
            if (!tableList.get(i).tryWriteLockIfExist(timeout, unit)) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(j).writeUnlock();
                }
                return false;
            }
        }
        return true;
    }

    public static void writeLockTablesOrMetaException(List<? extends TableIf> tableList) throws MetaNotFoundException {
        for (int i = 0; i < tableList.size(); i++) {
            try {
                tableList.get(i).writeLockOrMetaException();
            } catch (MetaNotFoundException e) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(j).writeUnlock();
                }
                throw e;
            }
        }
    }

    public static boolean tryWriteLockTablesOrMetaException(List<? extends TableIf> tableList, long timeout,
            TimeUnit unit) throws MetaNotFoundException {
        for (int i = 0; i < tableList.size(); i++) {
            try {
                if (!tableList.get(i).tryWriteLockOrMetaException(timeout, unit)) {
                    for (int j = i - 1; j >= 0; j--) {
                        tableList.get(j).writeUnlock();
                    }
                    return false;
                }
            } catch (MetaNotFoundException e) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(j).writeUnlock();
                }
                throw e;
            }
        }
        return true;
    }

    public static void writeUnlockTables(List<? extends TableIf> tableList) {
        for (int i = tableList.size() - 1; i >= 0; i--) {
            tableList.get(i).writeUnlock();
        }
    }

    public static void commitLockTables(List<Table> tableList) {
        for (Table table : tableList) {
            table.commitLock();
        }
    }

    public static void commitUnlockTables(List<Table> tableList) {
        for (int i = tableList.size() - 1; i >= 0; i--) {
            tableList.get(i).commitUnlock();
        }
    }

    public static boolean tryCommitLockTables(List<Table> tableList, long timeout, TimeUnit unit) {
        for (int i = 0; i < tableList.size(); i++) {
            if (!tableList.get(i).tryCommitLock(timeout, unit)) {
                for (int j = i - 1; j >= 0; j--) {
                    tableList.get(j).commitUnlock();
                }
                return false;
            }
        }
        return true;
    }
}
