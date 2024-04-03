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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.Executor;

public class HudiPartitionMgr {
    private static volatile HudiPartitionMgr partitionMgr = null;

    private static final Map<Long, HudiPartitionProcessor> partitionProcessors = Maps.newConcurrentMap();
    private final Executor executor;

    private HudiPartitionMgr(Executor executor) {
        this.executor = executor;
    }

    public HudiPartitionProcessor getPartitionProcessor(ExternalCatalog catalog) {
        return partitionProcessors.computeIfAbsent(catalog.getId(), catalogId -> {
            if (catalog instanceof HMSExternalCatalog) {
                return new HudiCachedPartitionProcessor(catalogId, executor);
            } else {
                throw new RuntimeException("Hudi only supports hive(or compatible) catalog now");
            }
        });
    }

    public void removePartitionProcessor(long catalogId) {
        HudiPartitionProcessor processor = partitionProcessors.remove(catalogId);
        if (processor != null) {
            processor.cleanUp();
        }
    }

    public void cleanPartitionProcess(long catalogId) {
        HudiPartitionProcessor processor = partitionProcessors.get(catalogId);
        if (processor != null) {
            processor.cleanUp();
        }
    }

    public void cleanDatabasePartitions(long catalogId, String dbName) {
        HudiPartitionProcessor processor = partitionProcessors.get(catalogId);
        if (processor != null) {
            processor.cleanDatabasePartitions(dbName);
        }
    }

    public void cleanTablePartitions(long catalogId, String dbName, String tblName) {
        HudiPartitionProcessor processor = partitionProcessors.get(catalogId);
        if (processor != null) {
            processor.cleanTablePartitions(dbName, tblName);
        }
    }

    public static HudiPartitionMgr get(Executor executor) {
        if (partitionMgr == null) {
            synchronized (HudiPartitionMgr.class) {
                if (partitionMgr == null) {
                    partitionMgr = new HudiPartitionMgr(executor);
                }
            }
        }
        return partitionMgr;
    }
}
