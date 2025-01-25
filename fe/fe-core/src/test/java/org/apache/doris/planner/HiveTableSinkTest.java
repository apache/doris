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

package org.apache.doris.planner;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.ThriftHMSCachedClient;

import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class HiveTableSinkTest {

    @Test
    public void testBindDataSink() throws AnalysisException {

        new MockUp<ThriftHMSCachedClient>() {
            @Mock
            List<Partition> listPartitions(String dbName, String tblName) {
                return new ArrayList<Partition>() {{
                        add(new Partition() {{
                                setValues(new ArrayList<String>() {{
                                        add("a");
                                    }
                                });
                                setSd(new StorageDescriptor() {{
                                        setInputFormat("orc");
                                    }
                                });
                            }
                        });
                    }
                };
            }
        };

        new MockUp<HMSExternalCatalog>() {
            @Mock
            public HMSCachedClient getClient() {
                return new ThriftHMSCachedClient(null, 2);
            }
        };

        ArrayList<String> locations = new ArrayList<String>() {{
                add("gs://abc/def");
                add("s3://abc/def");
                add("s3a://abc/def");
                add("s3n://abc/def");
                add("bos://abc/def");
                add("oss://abc/def");
                add("cos://abc/def");
            }
        };
        for (String location : locations) {
            mockDifferLocationTable(location);

            HMSExternalCatalog hmsExternalCatalog = new HMSExternalCatalog();
            hmsExternalCatalog.setInitialized(true);
            HMSExternalDatabase db = new HMSExternalDatabase(hmsExternalCatalog, 10000, "hive_db1", "hive_db1");
            HMSExternalTable tbl = new HMSExternalTable(10001, "hive_tbl1", "hive_db1", hmsExternalCatalog, db);
            HiveTableSink hiveTableSink = new HiveTableSink(tbl);
            hiveTableSink.bindDataSink(Optional.empty());

            Assert.assertEquals(hiveTableSink.tDataSink.hive_table_sink.location.original_write_path, location);
            Assert.assertEquals(hiveTableSink.tDataSink.hive_table_sink.location.target_path, location);
        }
    }

    private void mockDifferLocationTable(String location) {
        new MockUp<HMSExternalTable>() {
            @Mock
            public Set<String> getPartitionColumnNames() {
                return new HashSet<String>() {{
                        add("a");
                        add("b");
                    }
                };
            }

            @Mock
            public List<Column> getColumns() {
                Column a = new Column("a", PrimitiveType.INT);
                Column b = new Column("b", PrimitiveType.INT);
                return new ArrayList<Column>() {{
                        add(a);
                        add(b);
                    }
                };
            }

            @Mock
            public org.apache.hadoop.hive.metastore.api.Table getRemoteTable() {
                Table table = new Table();
                table.setSd(new StorageDescriptor() {{
                        setInputFormat("orc");
                        setBucketCols(new ArrayList<>());
                        setNumBuckets(1);
                        setSerdeInfo(new SerDeInfo() {{
                                setParameters(new HashMap<>());
                            }
                        });
                        setLocation(location);
                    }
                });
                table.setParameters(new HashMap<String, String>() {{
                        put("orc.compress", "lzo");
                    }
                });
                return table;
            }
        };
    }
}
