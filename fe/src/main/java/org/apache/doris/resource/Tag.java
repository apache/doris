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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;

/*
 * A Tag consists of type and name.
 * Tag type and name are both case insensitive, and represented in lower case.
 * Tag is printed as { "type": "name" }
 * 
 * Tag is immutable once it being created.
 */
public class Tag {
    public enum Type {
        TYPE, FUNCTION, LOCATION, CUSTOM;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }
    
    public static final ImmutableSet<String> RESERVED_TAG_NAMES = ImmutableSet.of(
            "frontend", "backend", "broker", "remote_storage", "store", "computation", "default_cluster");
    private static final String TAG_NAME_REGEX = "^[a-z][a-z0-9_]{0,32}$";

    public final Type type;
    public final String tag;

    private Tag(Type type, String tag) {
        this.type = type;
        this.tag = tag.toLowerCase();
    }

    public static Tag create(String typeName, String tagName) throws AnalysisException {
        try {
            Type tagType = Type.valueOf(typeName.toUpperCase());
            tagName = tagName.toLowerCase();
            if (Strings.isNullOrEmpty(tagName) || !tagName.matches(TAG_NAME_REGEX)) {
                throw new AnalysisException("Invalid tag name: " + tagName);
            }

            return create(tagType, tagName);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("Invalid tag type: " + typeName);
        }
    }

    public static Tag create(Type type, String tagName) {
        return new Tag(type, tagName);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(type, tag);
    }
    
    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        if (!(other instanceof Tag)) {
            return false;
        }
        Tag otherTag = (Tag) other;
        return type == otherTag.type && Objects.equals(tag, otherTag.tag);
    }

    @Override
    public String toString() {
        return "{\"" + type.toString() + "\" : \"" + tag + "\"}";
    }
}
