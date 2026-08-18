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

package org.apache.doris.connector.paimon;

import org.apache.doris.kerberos.HadoopAuthenticator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class PaimonHmsCatalogTest {

    @Test
    public void warehouseCheckUsesStorageIdentityAndLoaderReinstallsHmsBoundary() throws Exception {
        RecordingFileIO fileIO = new RecordingFileIO();
        FileIOLoader loader = new FileIOLoader() {
            @Override
            public String getScheme() {
                return "record";
            }

            @Override
            public FileIO load(Path path) {
                return fileIO;
            }
        };
        Options options = Options.fromMap(Map.of(
                "metastore", "hive",
                "warehouse", "record:///warehouse",
                "uri", "thrift://hms:9083",
                "cache-enabled", "false"));
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.sasl.enabled", "true");
        CatalogContext catalogContext = CatalogContext.create(options, hiveConf, loader, null);
        Map<String, String> properties = new HashMap<>(Map.of(
                "paimon.catalog.type", "hms",
                "warehouse", "record:///warehouse",
                "hive.metastore.uris", "thrift://hms:9083",
                "hive.metastore.authentication.type", "simple",
                "hive.metastore.username", "paimon-hms-user"));
        HadoopAuthenticator hmsAuth = PaimonConnector.buildHmsAuthenticator(properties, new HashMap<>());
        Configuration storageConf = new Configuration(false);
        storageConf.set("hadoop.username", "paimon-storage-user");
        HadoopAuthenticator storageAuth = HadoopAuthenticator.getHadoopAuthenticator(storageConf);

        Catalog catalog = storageAuth.doAs(() -> PaimonConnector.createHmsCatalog(
                catalogContext, hmsAuth, properties, new HashMap<>()));
        Assertions.assertEquals("paimon-storage-user", fileIO.checkUser);

        Catalog loaded = storageAuth.doAs(() -> catalog.catalogLoader().load());
        HiveCatalog loadedRoot = (HiveCatalog) DelegateCatalog.rootCatalog(loaded);
        Field clients = HiveCatalog.class.getDeclaredField("clients");
        clients.setAccessible(true);
        Assertions.assertInstanceOf(PaimonHmsClientPool.class, clients.get(loadedRoot));
    }

    private static final class RecordingFileIO extends LocalFileIO {
        private String checkUser;

        @Override
        public boolean exists(Path path) throws IOException {
            checkUser = UserGroupInformation.getCurrentUser().getUserName();
            return !path.toString().endsWith("user.sys");
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            checkUser = UserGroupInformation.getCurrentUser().getUserName();
            return new FileStatus() {
                @Override
                public long getLen() {
                    return 0;
                }

                @Override
                public boolean isDir() {
                    return true;
                }

                @Override
                public Path getPath() {
                    return path;
                }

                @Override
                public long getModificationTime() {
                    return 0;
                }
            };
        }
    }
}
