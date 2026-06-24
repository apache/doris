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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.ThriftHMSCachedClient;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.util.PathUtils;
import org.apache.doris.qe.ConnectContext;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HiveTableSinkTest {

    @Test
    public void testBindDataSink() throws UserException {
        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();

        List<Partition> partitions = new ArrayList<Partition>() {{
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

        Map<String, String> storageProperties = new HashMap<>();
        storageProperties.put("oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        storageProperties.put("oss.access_key", "access_key");
        storageProperties.put("oss.secret_key", "secret_key");
        storageProperties.put("s3.endpoint", "s3.north-1.amazonaws.com");
        storageProperties.put("s3.access_key", "access_key");
        storageProperties.put("s3.secret_key", "secret_key");
        storageProperties.put("cos.endpoint", "cos.cn-hangzhou.myqcloud.com");
        storageProperties.put("cos.access_key", "access_key");
        storageProperties.put("cos.secret_key", "secret_key");
        List<StorageProperties> storagePropertiesList = StorageProperties.createAll(storageProperties);
        Map<StorageProperties.Type, StorageProperties> storagePropertiesMap = storagePropertiesList.stream()
                .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));

        ThriftHMSCachedClient mockClient = Mockito.mock(ThriftHMSCachedClient.class);
        Mockito.when(mockClient.listPartitions(Mockito.anyString(), Mockito.anyString())).thenReturn(partitions);

        ArrayList<String> locations = new ArrayList<String>() {{
                add("oss://abc/def");
                add("s3://abc/def");
                add("s3a://abc/def");
                add("s3n://abc/def");
                add("cos://abc/def");
            }
        };
        for (String location : locations) {
            HMSExternalCatalog hmsExternalCatalog = Mockito.spy(new HMSExternalCatalog());
            hmsExternalCatalog.setInitializedForTest(true);
            Mockito.doReturn(mockClient).when(hmsExternalCatalog).getClient();

            HMSExternalDatabase db = new HMSExternalDatabase(hmsExternalCatalog, 10000, "hive_db1", "hive_db1");
            HMSExternalTable tbl = Mockito.spy(new HMSExternalTable(10001, "hive_tbl1", "hive_db1",
                    hmsExternalCatalog, db));
            Mockito.doReturn(storagePropertiesMap).when(tbl).getStoragePropertiesMap();
            mockDifferLocationTable(tbl, location);

            HiveTableSink hiveTableSink = new HiveTableSink(tbl);
            hiveTableSink.bindDataSink(Optional.empty());
            Assert.assertTrue(PathUtils.equalsIgnoreSchemeIfOneIsS3(hiveTableSink.tDataSink.hive_table_sink.location.write_path, location));
        }
    }

    private void mockDifferLocationTable(HMSExternalTable tbl, String location) {
        Mockito.doReturn(false).when(tbl).isPartitionedTable();

        Mockito.doReturn(new HashSet<String>() {{
                add("a");
                add("b");
            }
        }).when(tbl).getPartitionColumnNames();

        Column a = new Column("a", PrimitiveType.INT);
        Column b = new Column("b", PrimitiveType.INT);
        Mockito.doReturn(new ArrayList<Column>() {{
                add(a);
                add(b);
            }
        }).when(tbl).getColumns();

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
        Mockito.doReturn(table).when(tbl).getRemoteTable();
    }
}
