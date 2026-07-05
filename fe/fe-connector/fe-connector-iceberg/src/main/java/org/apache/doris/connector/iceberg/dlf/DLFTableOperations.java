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

package org.apache.doris.connector.iceberg.dlf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.FileIO;
import shade.doris.hive.org.apache.thrift.TException;

/**
 * Iceberg table operations against an Aliyun DLF (hive-compatible) metastore. Ported verbatim from the legacy
 * fe-core {@code org.apache.doris.datasource.iceberg.dlf.DLFTableOperations} into the plugin connector
 * (P6-T07); no fe-core dependencies (iceberg-hive + relocated thrift from hive-catalog-shade).
 */
public class DLFTableOperations extends HiveTableOperations {

    public DLFTableOperations(Configuration conf,
                              ClientPool<IMetaStoreClient, TException> metaClients,
                              FileIO fileIO,
                              String catalogName,
                              String database,
                              String table) {
        super(conf, metaClients, fileIO, catalogName, database, table);
    }
}
