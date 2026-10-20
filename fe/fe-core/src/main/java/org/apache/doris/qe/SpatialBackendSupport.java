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

package org.apache.doris.qe;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarType;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/** Reject spatial plans before either coordinator sends its first fragment RPC. */
public final class SpatialBackendSupport {
    private SpatialBackendSupport() {
    }

    public static void check(Collection<?> plans, Collection<Backend> backends) {
        if (backends.stream().allMatch(Backend::isSpatialTypesSupported) || !containsSpatial(plans)) {
            return;
        }
        for (Backend backend : backends) {
            if (!backend.isSpatialTypesSupported()) {
                throw new AnalysisException("Backend " + backend.getId() + " (" + backend.getHost()
                        + ") does not support spatial types; upgrade it before executing this query");
            }
        }
    }

    private static boolean containsSpatial(Collection<?> plans) {
        ArrayDeque<Object> pending = new ArrayDeque<>(plans);
        Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        while (!pending.isEmpty()) {
            Object value = pending.removeLast();
            if (!visited.add(value)) {
                continue;
            }
            if (value instanceof TScalarType) {
                TPrimitiveType type = ((TScalarType) value).getType();
                if (type == TPrimitiveType.GEOMETRY || type == TPrimitiveType.GEOGRAPHY) {
                    return true;
                }
            }
            // Inspect the actual wire plan: checking result slots alone misses, for example,
            // ST_AsBinary(spatial_constructor(...)) inside a predicate with no spatial scan slots.
            if (value instanceof TBase) {
                addFields((TBase<?, ?>) value, pending);
            } else if (value instanceof Collection) {
                pending.addAll((Collection<?>) value);
            } else if (value instanceof Map) {
                pending.addAll(((Map<?, ?>) value).values());
            }
        }
        return false;
    }

    private static <F extends TFieldIdEnum> void addFields(TBase<?, F> value, ArrayDeque<Object> pending) {
        Map<? extends TFieldIdEnum, FieldMetaData> fields = FieldMetaData.getStructMetaDataMap(value.getClass());
        for (TFieldIdEnum key : fields.keySet()) {
            F field = value.fieldForId(key.getThriftFieldId());
            if (value.isSet(field)) {
                Object child = value.getFieldValue(field);
                if (child instanceof TBase || child instanceof Collection || child instanceof Map) {
                    pending.add(child);
                }
            }
        }
    }
}
