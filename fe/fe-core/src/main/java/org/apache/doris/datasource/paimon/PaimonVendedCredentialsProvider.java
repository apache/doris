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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.credentials.AbstractVendedCredentialsProvider;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonRestMetaStoreProperties;

import com.google.common.collect.Maps;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.Table;

import java.util.Map;

public class PaimonVendedCredentialsProvider extends AbstractVendedCredentialsProvider {
    private static final PaimonVendedCredentialsProvider INSTANCE = new PaimonVendedCredentialsProvider();

    private PaimonVendedCredentialsProvider() {
        // Singleton pattern
    }

    public static PaimonVendedCredentialsProvider getInstance() {
        return INSTANCE;
    }

    @Override
    protected boolean isVendedCredentialsEnabled(MetastoreProperties metastoreProperties) {
        // Paimon REST catalog always supports vended credentials if it's REST type
        return metastoreProperties instanceof PaimonRestMetaStoreProperties;
    }

    @Override
    protected <T> Map<String, String> extractRawVendedCredentials(T tableObject) {
        if (!(tableObject instanceof Table)) {
            return Maps.newHashMap();
        }

        Table table = (Table) tableObject;
        if (table.fileIO() == null || !(table.fileIO() instanceof RESTTokenFileIO)) {
            return Maps.newHashMap();
        }

        RESTTokenFileIO restTokenFileIO = (RESTTokenFileIO) table.fileIO();
        RESTToken restToken = restTokenFileIO.validToken();
        Map<String, String> tokens = restToken.token();

        // Convert the original token to OSS format properties, let StorageProperties.createAll() further convert
        Map<String, String> rawProperties = Maps.newHashMap();
        rawProperties.putAll(tokens);

        return rawProperties;
    }

    @Override
    protected <T> String getTableName(T tableObject) {
        if (tableObject instanceof Table) {
            return ((Table) tableObject).name();
        }
        return super.getTableName(tableObject);
    }
}
