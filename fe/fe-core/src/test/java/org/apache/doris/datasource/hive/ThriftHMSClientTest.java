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

package org.apache.doris.datasource.hive;

import org.apache.doris.datasource.property.constants.HMSProperties;

import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ThriftHMSClientTest {
    @Mocked
    private IMetaStoreClient mockClient;
    @Mocked
    private HiveConf mockHiveConf;

    private ThriftHMSCachedClient.ThriftHMSClient thriftHMSClient;
    private ThriftHMSCachedClient cachedClient;

    @Before
    public void setUp() throws Exception {
        // Mock basic HiveConf properties
        new Expectations() {
            {
                mockHiveConf.get(HMSProperties.HIVE_METASTORE_TYPE);
                result = "test";
            }
        };

        cachedClient = new ThriftHMSCachedClient(mockHiveConf, 2);
        thriftHMSClient = cachedClient.new ThriftHMSClient(mockHiveConf);
    }

    @Test
    public void testCloseSuccessfullyReturnsToPool() throws Exception {
        // When
        thriftHMSClient.close();

        // Then
        Assert.assertTrue(thriftHMSClient.isInPool());
        Assert.assertFalse(thriftHMSClient.isClosed());
    }

    @Test
    public void testCloseWithThrowableClosesClient() throws Exception {
        // Given
        thriftHMSClient.setThrowable(new RuntimeException("Test exception"));

        // When
        thriftHMSClient.close();

        // Then
        Assert.assertFalse(thriftHMSClient.isInPool());
        Assert.assertTrue(thriftHMSClient.isClosed());
    }

    @Test
    public void testCloseWhenPoolIsFullClosesClient() throws Exception {
        // Fill up the pool
        ThriftHMSCachedClient.ThriftHMSClient client1 = cachedClient.new ThriftHMSClient(mockHiveConf);
        ThriftHMSCachedClient.ThriftHMSClient client2 = cachedClient.new ThriftHMSClient(mockHiveConf);
        client1.close();
        client2.close();

        // When
        thriftHMSClient.close();

        // Then
        Assert.assertFalse(thriftHMSClient.isInPool());
        Assert.assertTrue(thriftHMSClient.isClosed());
    }
}
