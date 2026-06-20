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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.security.authentication.HadoopExecutionAuthenticator;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import lombok.Getter;

import java.util.List;
import java.util.Map;

public abstract class AbstractPaimonProperties extends MetastoreProperties {
    @ConnectorProperty(
            names = {"warehouse"},
            description = "The location of the Paimon warehouse. This is where the tables will be stored."
    )
    protected String warehouse;

    @Getter
    protected ExecutionAuthenticator executionAuthenticator = new ExecutionAuthenticator() {
    };

    public abstract String getPaimonCatalogType();

    protected AbstractPaimonProperties(Map<String, String> props) {
        super(Type.PAIMON, props);
    }

    /**
     * Builds the HDFS Kerberos {@link ExecutionAuthenticator} from the catalog's storage properties.
     * Shared by the filesystem and jdbc flavors' {@link #initExecutionAuthenticator} override so the
     * plugin/cutover path wires a real {@code doAs} authenticator over Kerberized HDFS. No-op when
     * there is no HDFS storage (e.g. an S3-backed warehouse) — leaving the base no-op authenticator,
     * which is correct (no Kerberos UGI to apply).
     */
    protected void initHdfsExecutionAuthenticator(List<StorageProperties> storagePropertiesList) {
        if (storagePropertiesList == null) {
            return;
        }
        for (StorageProperties sp : storagePropertiesList) {
            if (sp.getType() == StorageProperties.Type.HDFS) {
                this.executionAuthenticator = new HadoopExecutionAuthenticator(
                        ((HdfsProperties) sp).getHadoopAuthenticator());
                return;
            }
        }
    }
}
