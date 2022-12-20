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

package org.apache.doris.catalog;

import org.apache.doris.alter.AlterCancelException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.statistics.AnalysisTaskInfo;
import org.apache.doris.statistics.AnalysisTaskScheduler;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.thrift.TTableDescriptor;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface TableIf {

    void readLock();

    boolean tryReadLock(long timeout, TimeUnit unit);

    void readUnlock();

    void writeLock();

    boolean writeLockIfExist();

    boolean tryWriteLock(long timeout, TimeUnit unit);

    void writeUnlock();

    boolean isWriteLockHeldByCurrentThread();

    <E extends Exception> void writeLockOrException(E e) throws E;

    void writeLockOrDdlException() throws DdlException;

    void writeLockOrMetaException() throws MetaNotFoundException;

    void writeLockOrAlterCancelException() throws AlterCancelException;

    boolean tryWriteLockOrMetaException(long timeout, TimeUnit unit) throws MetaNotFoundException;

    <E extends Exception> boolean tryWriteLockOrException(long timeout, TimeUnit unit, E e) throws E;

    boolean tryWriteLockIfExist(long timeout, TimeUnit unit);

    long getId();

    String getName();

    TableType getType();

    List<Column> getFullSchema();

    List<Column> getBaseSchema();

    List<Column> getBaseSchema(boolean full);

    void setNewFullSchema(List<Column> newSchema);

    Column getColumn(String name);

    default int getBaseColumnIdxByName(String colName) {
        int i = 0;
        for (Column col : getBaseSchema()) {
            if (col.getName().equalsIgnoreCase(colName)) {
                return i;
            }
            ++i;
        }
        return -1;
    }

    String getMysqlType();

    String getEngine();

    String getComment();

    long getCreateTime();

    long getUpdateTime();

    long getRowCount();

    long getDataLength();

    long getAvgRowLength();

    long getLastCheckTime();

    String getComment(boolean escapeQuota);

    TTableDescriptor toThrift();

    BaseAnalysisTask createAnalysisTask(AnalysisTaskScheduler scheduler, AnalysisTaskInfo info);

    /**
     * Doris table type.
     */
    enum TableType {
        MYSQL, ODBC, OLAP, SCHEMA, INLINE_VIEW, VIEW, BROKER, ELASTICSEARCH, HIVE, ICEBERG, HUDI, JDBC,
        TABLE_VALUED_FUNCTION, HMS_EXTERNAL_TABLE, ES_EXTERNAL_TABLE, MATERIALIZED_VIEW, JDBC_EXTERNAL_TABLE;

        public String toEngineName() {
            switch (this) {
                case MYSQL:
                    return "MySQL";
                case ODBC:
                    return "Odbc";
                case OLAP:
                    return "Doris";
                case SCHEMA:
                    return "MEMORY";
                case INLINE_VIEW:
                    return "InlineView";
                case VIEW:
                    return "View";
                case BROKER:
                    return "Broker";
                case ELASTICSEARCH:
                    return "ElasticSearch";
                case HIVE:
                    return "Hive";
                case HUDI:
                    return "Hudi";
                case JDBC:
                case JDBC_EXTERNAL_TABLE:
                    return "jdbc";
                case TABLE_VALUED_FUNCTION:
                    return "Table_Valued_Function";
                case HMS_EXTERNAL_TABLE:
                    return "hms";
                case ES_EXTERNAL_TABLE:
                    return "es";
                default:
                    return null;
            }
        }

        public String toMysqlType() {
            switch (this) {
                case OLAP:
                    return "BASE TABLE";
                case SCHEMA:
                    return "SYSTEM VIEW";
                case INLINE_VIEW:
                case VIEW:
                case MATERIALIZED_VIEW:
                    return "VIEW";
                case MYSQL:
                case ODBC:
                case BROKER:
                case ELASTICSEARCH:
                case HIVE:
                case HUDI:
                case JDBC:
                case JDBC_EXTERNAL_TABLE:
                case TABLE_VALUED_FUNCTION:
                case HMS_EXTERNAL_TABLE:
                case ES_EXTERNAL_TABLE:
                    return "EXTERNAL TABLE";
                default:
                    return null;
            }
        }
    }

    default List<Column> getColumns() {
        return Collections.emptyList();
    }

    default Set<String> getPartitionNames() {
        return Collections.emptySet();
    }

    default Partition getPartition(String name) {
        return null;
    }
}

