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

import org.apache.doris.common.UserException;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class IcebergPropertiesFactory implements MetastorePropertiesFactory {

    private static final Map<String, Function<Map<String, String>, MetastoreProperties>> REGISTERED_SUBTYPES =
            new HashMap<>();

    static {
        register("rest", IcebergRestProperties::new);
    }

    public static void register(String subType, Function<Map<String, String>, MetastoreProperties> constructor) {
        REGISTERED_SUBTYPES.put(subType.toLowerCase(Locale.ROOT), constructor);
    }

    @Override
    public MetastoreProperties create(Map<String, String> props) throws UserException {
        String subType = props.getOrDefault("iceberg.catalog.type", "rest").toLowerCase(Locale.ROOT);
        Function<Map<String, String>, MetastoreProperties> constructor =
                REGISTERED_SUBTYPES.getOrDefault(subType, REGISTERED_SUBTYPES.get("rest"));
        MetastoreProperties instance = constructor.apply(props);
        instance.initNormalizeAndCheckProps();
        return instance;
    }
}
