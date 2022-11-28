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

package org.apache.doris.job;

import org.apache.doris.common.io.Writable;

import com.google.gson.annotations.SerializedName;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class ResourceQueueConfig implements Writable {
    public static final String MAX_CONCURRENCY = "max_concurrency";
    public static final String MAX_QUEUE_SIZE = "max_queue_size";

    private int maxConcurrency;
    private int maxQueueSize;

    @SerializedName(value = "properties")
    private final Map<String, String> properties;

    public ResourceQueueConfig(Map<String, String> properties) {
        this.properties = properties;
        try {
            maxConcurrency = Integer.parseInt(properties.get(MAX_CONCURRENCY));
        } catch (Exception e) {
            maxConcurrency = 0;
        }
        if (maxConcurrency < 1) {
            properties.put(MAX_CONCURRENCY, "1");
            maxConcurrency = 1;
        }
        try {
            maxQueueSize = Integer.parseInt(properties.get(MAX_QUEUE_SIZE));
        } catch (Exception e) {
            maxQueueSize = 0;
        }
        if (maxQueueSize < 1) {
            properties.put(MAX_QUEUE_SIZE, "1");
            maxQueueSize = 1;
        }
    }

    public String getOrDefault(String key, String defaultVal) {
        return properties.getOrDefault(key, defaultVal);
    }

    public String set(String key, String value) {
        return properties.put(key, value);
    }

    public int maxConcurrency() {
        return maxConcurrency;
    }

    public int maxQueueSize() {
        return maxQueueSize;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public String toString() {
        return properties.entrySet().stream().map(
                        kv -> String.format("\"%s\" = \"%s\"", kv.getKey(), kv.getValue()))
                .collect(Collectors.joining(", "));
    }
}
