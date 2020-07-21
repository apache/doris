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
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
    public static final String TYPE_FUNCATION = "function";
    public static final String TYPE_LOCATION = "location";

    public static final String VALUE_FRONTEND = "frontend";
    public static final String VALUE_BACKEND = "backend";
    public static final String VALUE_BROKER = "broker";
    public static final String VALUE_REMOTE_STORAGE = "remote_storage";
    public static final String VALUE_STORE = "store";
    public static final String VALUE_COMPUTATION = "computation";
    public static final String VALUE_DEFAULT_CLUSTER = "default_cluster";

    public static final ImmutableSet<String> RESERVED_TAG_TYPE = ImmutableSet.of(
            TYPE_ROLE, TYPE_FUNCATION, TYPE_LOCATION);
    public static final ImmutableSet<String> RESERVED_TAG_VALUES = ImmutableSet.of(
            VALUE_FRONTEND, VALUE_BACKEND, VALUE_BROKER, VALUE_REMOTE_STORAGE, VALUE_STORE, VALUE_COMPUTATION,
            VALUE_DEFAULT_CLUSTER);
    private static final String TAG_REGEX = "^[a-z][a-z0-9_]{0,32}$";

    @SerializedName(value = "type")
    public String type;
    @SerializedName(value = "value")
    public String value;

    private Tag(String type, String val) {
        this.type = type.toLowerCase();
        this.value = val.toLowerCase();
    }

    public static Tag create(String type, String value) throws AnalysisException {
        if (!type.matches(TAG_REGEX) || !value.matches(TAG_REGEX)) {
            throw new AnalysisException("Invalid tag format: " + type + ":" + value);
        }
        return new Tag(type, value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }
    
    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        if (!(other instanceof Tag)) {
            return false;
        }
        Tag otherTag = (Tag) other;
        return type.equalsIgnoreCase(otherTag.type) && value.equalsIgnoreCase(otherTag.value);
    }

    @Override
    public String toString() {
        return "{\"" + type.toString() + "\" : \"" + value + "\"}";
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
