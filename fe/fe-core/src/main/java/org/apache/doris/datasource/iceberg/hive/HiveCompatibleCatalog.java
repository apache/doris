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

package org.apache.doris.datasource.iceberg.hive;

import org.apache.doris.datasource.iceberg.hadoop.IcebergHadoopFileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;
import shade.doris.hive.org.apache.thrift.TException;

import java.io.IOException;
import java.util.Map;

public abstract class HiveCompatibleCatalog extends HiveCatalog {

    protected Configuration conf;
    protected ClientPool<IMetaStoreClient, TException> clients;
    protected FileIO fileIO;
    protected String uid;

    public void initialize(String name, FileIO fileIO,
                           ClientPool<IMetaStoreClient, TException> clients) {
        this.uid = name;
        this.fileIO = fileIO;
        this.clients = clients;
    }

    protected FileIO initializeFileIO(Map<String, String> properties) {
        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        if (fileIOImpl == null) {
            /* when use the S3FileIO, we need some custom configurations,
             * so HadoopFileIO is used in the superclass by default
             * we can add better implementations to derived class just like the implementation in DLFCatalog.
             */
            FileIO io;
            try {
                FileSystem fs = getFileSystem();
                if (fs == null) {
                    io = new HadoopFileIO(getConf());
                } else {
                    io = new IcebergHadoopFileIO(getConf(), getFileSystem());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            io.initialize(properties);
            return io;
        } else {
            return CatalogUtil.loadFileIO(fileIOImpl, properties, getConf());
        }
    }

    public FileSystem getFileSystem() throws IOException {
        return null;
    }

    @Override
    public TableOperations newTableOps(TableIdentifier tableIdentifier) {
        String dbName = tableIdentifier.namespace().level(0);
        String tableName = tableIdentifier.name();
        return new IcebergHiveTableOperations(this.conf, this.clients, this.fileIO, this.uid, dbName, tableName);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        if (conf == null) {
            return new HdfsConfiguration();
        }
        return conf;
    }
}
