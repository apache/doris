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

package org.apache.doris.datasource.iceberg.broker;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.datasource.HMSExternalCatalog;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableMap;

import java.io.IOException;
import java.util.Map;

/**
 * FileIO implementation that uses broker to execute Iceberg files IO operation.
 */
public class IcebergBrokerIO implements FileIO {

    private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());
    private BrokerDesc brokerDesc = null;

    @Override
    public void initialize(Map<String, String> props) {
        this.properties = SerializableMap.copyOf(props);
        if (!properties.containsKey(HMSExternalCatalog.BIND_BROKER_NAME)) {
            throw new UnsupportedOperationException(String.format("No broker is specified, "
                    + "try to set '%s' in HMS Catalog", HMSExternalCatalog.BIND_BROKER_NAME));
        }
        String brokerName = properties.get(HMSExternalCatalog.BIND_BROKER_NAME);
        this.brokerDesc = new BrokerDesc(brokerName, properties.immutableMap());
    }

    @Override
    public Map<String, String> properties() {
        return properties.immutableMap();
    }

    @Override
    public InputFile newInputFile(String path) {
        if (brokerDesc == null) {
            throw new UnsupportedOperationException("IcebergBrokerIO should be initialized first");
        }
        try {
            return BrokerInputFile.create(path, brokerDesc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw new UnsupportedOperationException("IcebergBrokerIO does not support writing files");
    }

    @Override
    public void deleteFile(String path) {
        throw new UnsupportedOperationException("IcebergBrokerIO does not support deleting files");
    }

    @Override
    public void close() { }
}
