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

package org.apache.doris.catalog;

import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TRowTtlConfig;

import java.util.HashMap;
import java.util.Map;

/** Versioned GC policy for source-column row TTL tables. */
public class RowTtlConfig {
    public static final String TTL_PROPERTY = "function_column.ttl";
    public static final String VERSION_PROPERTY = "__row_ttl_config_version";

    private final long durationUs;
    private final long configVersion;

    public RowTtlConfig(long durationUs, long configVersion) {
        this.durationUs = durationUs;
        this.configVersion = configVersion;
    }

    public long getDurationUs() {
        return durationUs;
    }

    public long getConfigVersion() {
        return configVersion;
    }

    public Map<String, String> toProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(VERSION_PROPERTY, Long.toString(configVersion));
        return properties;
    }

    public TRowTtlConfig toThrift() {
        return new TRowTtlConfig().setDurationUs(durationUs).setConfigVersion(configVersion);
    }

    public OlapFile.RowTtlConfigPB toPb() {
        return OlapFile.RowTtlConfigPB.newBuilder().setDurationUs(durationUs)
                .setConfigVersion(configVersion).build();
    }
}
