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

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class IcebergPropertiesFactory implements MetastorePropertiesFactory {
    private static final Map<String, Function<Map<String, String>, MetastoreProperties>> REGISTERED_SUBTYPES =
            new HashMap<>();

    static {
        register("glue", IcebergGlueMetaStoreProperties::new);
        //register("rest", HMSProperties::new);
        register("hms", IcebergHMSMetaStoreProperties::new);
        register("hadoop", IcebergFileSystemMetaStoreProperties::new);
        register("s3tables", IcebergS3TablesMetaStoreProperties::new);
    }

    public static void register(String subType, Function<Map<String, String>, MetastoreProperties> constructor) {
        REGISTERED_SUBTYPES.put(subType.toLowerCase(Locale.ROOT), constructor);
    }

    @Override
    public MetastoreProperties create(Map<String, String> props) {
        String subType = props.get("iceberg.catalog.type").toLowerCase(Locale.ROOT);
        if (StringUtils.isBlank(subType)) {
            throw new IllegalArgumentException("iceberg.catalog.type is empty");
        }
        Function<Map<String, String>, MetastoreProperties> constructor =
                REGISTERED_SUBTYPES.get(subType);
        MetastoreProperties instance = constructor.apply(props);
        instance.initNormalizeAndCheckProps();
        return instance;
    }
}
