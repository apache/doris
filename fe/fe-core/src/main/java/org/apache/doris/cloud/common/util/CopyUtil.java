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

package org.apache.doris.cloud.common.util;

import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CopyUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CopyUtil.class);

    public static <T extends Writable, R extends T> R copyToChild(T parent, Class<R> childClass) {
        try {
            if (parent == null || childClass == null) {
                return null;
            }
            JsonObject childJson = new JsonObject();
            JsonObject parentJson = GsonUtils.GSON.toJsonTree(parent).getAsJsonObject();
            for (Map.Entry<String, JsonElement> e : parentJson.entrySet()) {
                childJson.add(e.getKey(), e.getValue());
            }
            if (childJson.has("clazz")) {
                childJson.add("clazz", new JsonPrimitive(childClass.getSimpleName()));
            }
            return (R) GsonUtils.GSON.<R>fromJson(childJson, childClass);
        } catch (Exception e) {
            LOG.warn("failed to copy from {} to {}", parent.getClass().getName(),
                    childClass.getName(), e);
            return null;
        }
    }
}
