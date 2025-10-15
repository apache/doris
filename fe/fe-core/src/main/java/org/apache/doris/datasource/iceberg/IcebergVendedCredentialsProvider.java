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

import org.apache.doris.datasource.credentials.AbstractVendedCredentialsProvider;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;

import com.google.common.collect.Maps;
import org.apache.iceberg.Table;

import java.util.Map;

public class IcebergVendedCredentialsProvider extends AbstractVendedCredentialsProvider {
    private static final IcebergVendedCredentialsProvider INSTANCE = new IcebergVendedCredentialsProvider();

    private IcebergVendedCredentialsProvider() {
        // Singleton pattern
    }

    public static IcebergVendedCredentialsProvider getInstance() {
        return INSTANCE;
    }

    @Override
    protected boolean isVendedCredentialsEnabled(MetastoreProperties metastoreProperties) {
        if (metastoreProperties instanceof IcebergRestProperties) {
            return ((IcebergRestProperties) metastoreProperties).isIcebergRestVendedCredentialsEnabled();
        }
        return false;
    }

    @Override
    protected <T> Map<String, String> extractRawVendedCredentials(T tableObject) {
        if (!(tableObject instanceof Table)) {
            return Maps.newHashMap();
        }

        Table table = (Table) tableObject;
        if (table.io() == null) {
            return Maps.newHashMap();
        }

        // Return table.io().properties() directly, and let StorageProperties.createAll() to convert the format
        return table.io().properties();
    }

    @Override
    protected <T> String getTableName(T tableObject) {
        if (tableObject instanceof Table) {
            return ((Table) tableObject).name();
        }
        return super.getTableName(tableObject);
    }
}
