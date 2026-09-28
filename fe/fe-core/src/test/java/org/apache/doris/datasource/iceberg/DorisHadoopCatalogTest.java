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

package org.apache.doris.datasource.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class DorisHadoopCatalogTest {

    public static class TrackingFileIO extends HadoopFileIO {
        private static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

        @Override
        public void close() {
            CLOSE_COUNT.incrementAndGet();
        }
    }

    @TempDir
    Path warehouse;

    @Test
    void closesOwnedFileIOOnceAcrossRepeatedRetirement() throws Exception {
        TrackingFileIO.CLOSE_COUNT.set(0);
        DorisHadoopCatalog catalog = newCatalog();

        catalog.close();
        catalog.close();

        Assertions.assertEquals(1, TrackingFileIO.CLOSE_COUNT.get());
    }

    @Test
    void closesFileIOWhenLockManagerInitializationFails() {
        TrackingFileIO.CLOSE_COUNT.set(0);
        Map<String, String> properties = properties();
        properties.put("lock-impl", "missing.LockManager");
        DorisHadoopCatalog catalog = new DorisHadoopCatalog();
        catalog.setConf(new Configuration());

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> catalog.initialize("test", properties));

        Assertions.assertEquals(1, TrackingFileIO.CLOSE_COUNT.get());
    }

    private DorisHadoopCatalog newCatalog() {
        DorisHadoopCatalog catalog = new DorisHadoopCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize("test", properties());
        return catalog;
    }

    private Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toUri().toString());
        properties.put(CatalogProperties.FILE_IO_IMPL, TrackingFileIO.class.getName());
        return properties;
    }
}
