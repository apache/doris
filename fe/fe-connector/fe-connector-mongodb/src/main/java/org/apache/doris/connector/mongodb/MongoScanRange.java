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

package org.apache.doris.connector.mongodb;

import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TMongoScanDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.Map;
import java.util.Optional;

final class MongoScanRange implements ConnectorScanRange {
    private static final long serialVersionUID = 1L;
    private final TMongoScanDesc descriptor;

    MongoScanRange(TMongoScanDesc descriptor) {
        this.descriptor = descriptor.deepCopy();
    }

    @Override
    public Optional<String> getPath() {
        return Optional.of("/dummyPath");
    }

    @Override
    public String getSplitIdentity() {
        return descriptor.getDatabase() + "." + descriptor.getCollection() + ":" + descriptor.getSchemaVersion();
    }

    @Override
    public String getFileFormat() {
        return "mongodb";
    }

    @Override
    public String getTableFormatType() {
        return "mongodb";
    }

    @Override
    public boolean isNativeReadRange() {
        return true;
    }

    @Override
    public Map<String, String> getProperties() {
        // Generic scan properties appear in EXPLAIN; credentials belong only to the typed RPC descriptor.
        return Map.of();
    }

    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        formatDesc.setMongodbParams(descriptor.deepCopy());
    }
}
