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

package org.apache.doris.analysis;

import org.apache.doris.load.EtlJobType;

import com.google.common.base.Preconditions;

import java.util.EnumMap;

public enum LoadType {

    UNKNOWN,
    NATIVE_INSERT,
    BROKER_LOAD,
    SPARK_LOAD,
    MYSQL_LOAD,
    ROUTINE_LOAD,
    STREAM_LOAD;

    private static final EnumMap<LoadType, EtlJobType> LOAD_TYPE_TO_ETL_TYPE = new EnumMap<>(LoadType.class);

    static {
        LOAD_TYPE_TO_ETL_TYPE.put(NATIVE_INSERT, EtlJobType.INSERT);
        LOAD_TYPE_TO_ETL_TYPE.put(BROKER_LOAD, EtlJobType.BROKER);
        LOAD_TYPE_TO_ETL_TYPE.put(SPARK_LOAD, EtlJobType.SPARK);
        LOAD_TYPE_TO_ETL_TYPE.put(MYSQL_LOAD, EtlJobType.LOCAL_FILE);
        // TODO(tsy): add routine load and stream load
    }

    public static EtlJobType getEtlJobType(LoadType loadType) {
        return Preconditions.checkNotNull(LOAD_TYPE_TO_ETL_TYPE.get(loadType));
    }

}
