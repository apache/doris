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

package org.apache.doris.load.routineload;

import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;

import java.util.Map;

/**
 * RoutineLoadDataSourcePropertyFactory is used to create data source properties
 * for routine load job.
 * <p>
 * Currently, we only support kafka data source.
 * If we want to support more data source, we can add more data source properties here.
 * And we can add more data source type in LoadDataSourceType.
 * Then we can use this factory to create data source properties.
 * </p>
 */
public class RoutineLoadDataSourcePropertyFactory {

    public static AbstractDataSourceProperties createDataSource(String type, Map<String, String> parameters,
                                                                boolean multiLoad) {
        if (type.equalsIgnoreCase(LoadDataSourceType.KAFKA.name())) {
            return new KafkaDataSourceProperties(parameters, multiLoad);
        }
        throw new IllegalArgumentException("Unknown routine load data source type: " + type);
    }

    public static AbstractDataSourceProperties createDataSource(String type, Map<String, String> parameters) {
        if (type.equalsIgnoreCase(LoadDataSourceType.KAFKA.name())) {
            return new KafkaDataSourceProperties(parameters);
        }
        throw new IllegalArgumentException("Unknown routine load data source type: " + type);
    }
}
