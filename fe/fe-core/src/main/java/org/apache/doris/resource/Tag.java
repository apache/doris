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

package org.apache.doris.resource;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/*
 * A Tag consists of type and value.
 * Tag type and value are both case insensitive, and represented in lower case.
 * Tag is printed as { "type": "value" }
 *
 * Type is mainly used to categorize a tag. For example, users can customize a certain type of tag.
 * And these tags all use the same type. So user can quickly find this type of tags by the type.
 * Doris reserves several built-in types:
 *     ROLE: the role of resource, such as FRONTEND, BACKEND, BROKER
 *     FUNCTION: the function of a tag, such as STORAGE, COMPUTATION
 *     LOCATION: A type of tags representing location information.
 *
 * Value is customized. And Doris also reserves several built-in values for built-in types:
 *     FRONTEND, BACKEND, BROKER of type ROLE.
 *     REMOTE_STORAGE, STORAGE, COMPUTATION for type FUNCTION.
 *
 * A Tag is immutable once it being created.
 */
public class Tag implements Writable {

    public static final String TYPE_ROLE = "role";
    public static final String TYPE_FUNCTION = "function";
    public static final String TYPE_LOCATION = "location";
    public static final String VALUE_FRONTEND = "frontend";
    public static final String VALUE_BACKEND = "backend";
    public static final String VALUE_BROKER = "broker";
    public static final String VALUE_REMOTE_STORAGE = "remote_storage";
    public static final String VALUE_STORE = "store";
    public static final String VALUE_COMPUTATION = "computation";
    public static final String VALUE_MIX = "mix";
    public static final String VALUE_DEFAULT_CLUSTER = "default_cluster";
    public static final String VALUE_DEFAULT_TAG = "default";
    public static final String VALUE_INVALID_TAG = "invalid";

    public static final String CLOUD_CLUSTER_NAME = "cloud_cluster_name";
    public static final String CLOUD_CLUSTER_ID = "cloud_cluster_id";
    public static final String CLOUD_UNIQUE_ID = "cloud_unique_id";
    public static final String CLOUD_CLUSTER_PUBLIC_ENDPOINT = "cloud_cluster_public_endpoint";
    public static final String CLOUD_CLUSTER_PRIVATE_ENDPOINT = "cloud_cluster_private_endpoint";
    public static final String CLOUD_CLUSTER_STATUS = "cloud_cluster_status";

    public static final String VALUE_DEFAULT_CLOUD_CLUSTER_NAME = "default_cluster";

    public static final String WORKLOAD_GROUP = "workload_group";

    public static final ImmutableSet<String> RESERVED_TAG_TYPE = ImmutableSet.of(
            TYPE_ROLE, TYPE_FUNCTION, TYPE_LOCATION);
    public static final ImmutableSet<String> RESERVED_TAG_VALUES = ImmutableSet.of(
            VALUE_FRONTEND, VALUE_BACKEND, VALUE_BROKER, VALUE_REMOTE_STORAGE, VALUE_STORE, VALUE_COMPUTATION,
            VALUE_MIX, VALUE_DEFAULT_CLUSTER);
    private static final String TAG_TYPE_REGEX = "^[a-z][a-z0-9_]{0,32}$";
    private static final String TAG_VALUE_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,32}$";


    public static final Tag DEFAULT_BACKEND_TAG;
    public static final Tag DEFAULT_NODE_ROLE_TAG;
    public static final Tag INVALID_TAG;

    static {
        DEFAULT_BACKEND_TAG = new Tag(TYPE_LOCATION, VALUE_DEFAULT_TAG);
        DEFAULT_NODE_ROLE_TAG = new Tag(TYPE_ROLE, VALUE_MIX);
        INVALID_TAG = new Tag(TYPE_LOCATION, VALUE_INVALID_TAG);
    }

    @SerializedName(value = "type")
    public String type;
    @SerializedName(value = "value")
    public String value;

    private Tag(String type, String val) {
        this.type = type;
        this.value = val;
    }

    public static Tag create(String type, String value) throws AnalysisException {
        if (!type.matches(TAG_TYPE_REGEX)) {
            throw new AnalysisException("Invalid tag type format: " + type);
        }
        if (!value.matches(TAG_VALUE_REGEX)) {
            throw new AnalysisException("Invalid tag value format: " + value);
        }
        return new Tag(type, value);
    }

    public static Tag createNotCheck(String type, String value) {
        return new Tag(type, value);
    }

    // only support be and cn node role tag for be.
    public static boolean validNodeRoleTag(String value) {
        return value != null && (value.equals(VALUE_MIX) || value.equals(VALUE_COMPUTATION));
    }

    public String toKey() {
        return type + "_" + value;
    }

    public Map<String, String> toMap() {
        Map<String, String> map = Maps.newHashMap();
        map.put(type, value);
        return map;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof Tag)) {
            return false;
        }
        Tag otherTag = (Tag) other;
        return type.equals(otherTag.type) && value.equals(otherTag.value);
    }

    @Override
    public String toString() {
        return "{\"" + type + "\" : \"" + value + "\"}";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static Tag read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Tag.class);
    }
}
