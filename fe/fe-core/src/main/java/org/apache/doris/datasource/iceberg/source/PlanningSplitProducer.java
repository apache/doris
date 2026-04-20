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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.spi.Split;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * FE-side split production strategy for Iceberg planning.
 */
public interface PlanningSplitProducer {

    interface Context {
        TableScan createTableScan() throws UserException;

        CloseableIterable<FileScanTask> planFileScanTask(TableScan scan);

        CloseableIterable<FileScanTask> splitPlanningTasks(List<FileScanTask> fileScanTasks);

        Split createPlanningSplit(FileScanTask task);

        void recordManifestCacheProfile();

        Optional<NotSupportedException> checkNotSupportedException(Exception e);

        ExecutionAuthenticator getExecutionAuthenticator();

        Executor getScheduleExecutor();

        List<String> getSourceTableQualifier();

        String getPlanningTableSchemaJson();

        PartitionSpec getPlanningPartitionSpec(int specId);
    }

    void start(int numBackends, SplitSink sink) throws UserException;

    default void stop() {
    }
}
