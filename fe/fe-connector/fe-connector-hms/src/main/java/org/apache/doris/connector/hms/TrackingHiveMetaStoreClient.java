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

package org.apache.doris.connector.hms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import shade.doris.hive.org.apache.thrift.TException;

import java.util.List;

/** Raw HMS client used under RetryingMetaStoreClient so every retry attempt is observable. */
public final class TrackingHiveMetaStoreClient extends HiveMetaStoreClient implements IMetaStoreClient {

    public TrackingHiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded)
            throws MetaException {
        super(conf, hookLoader, allowEmbedded);
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> partitionNames)
            throws TException {
        return HmsRemoteCallTracking.trackWireAttempt(
                () -> super.getPartitionsByNames(dbName, tableName, partitionNames));
    }
}
