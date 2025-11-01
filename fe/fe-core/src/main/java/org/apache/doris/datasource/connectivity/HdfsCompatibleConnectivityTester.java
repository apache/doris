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

package org.apache.doris.datasource.connectivity;

import org.apache.doris.datasource.property.storage.HdfsCompatibleProperties;
import org.apache.doris.thrift.TStorageBackendType;

public abstract class HdfsCompatibleConnectivityTester implements StorageConnectivityTester {
    protected final HdfsCompatibleProperties properties;

    public HdfsCompatibleConnectivityTester(HdfsCompatibleProperties properties) {
        this.properties = properties;
    }

    @Override
    public TStorageBackendType getStorageType() {
        return TStorageBackendType.HDFS;
    }

    @Override
    public String getTestType() {
        return "HDFS";
    }

    @Override
    public void testFeConnection() throws Exception {
        // TODO: Implement HDFS connectivity test in the future if needed
        // Currently, HDFS connectivity test is not required
    }

    @Override
    public void testBeConnection() throws Exception {
        // TODO: Implement HDFS connectivity test in the future if needed
        // Currently, HDFS connectivity test is not required
    }
}
