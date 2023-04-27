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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;

public class FunctionTypeDeducers {

    public interface TypeDeducer {
        public Type deduce(Type[] args);
    }

    public static final ImmutableMap<String, TypeDeducer> DEDUCERS = ImmutableMap.<String, TypeDeducer>builder()
            .put("named_struct", new NamedStructDeducer())
            .put("element_at", new ElementAtDeducer())
            .build();

    public static Type deduce(String fnName, Type[] args) {
        if (DEDUCERS.containsKey(fnName)) {
            return DEDUCERS.get(fnName).deduce(args);
        }
        return null;
    }

    public static class NamedStructDeducer implements TypeDeducer {
        @Override
        public Type deduce(Type[] args) {
            List<Type> evenArgs = Lists.newArrayList();
            for (int i = 0; i < args.length; i++) {
                if ((i & 1) == 1) {
                    evenArgs.add(args[i]);
                }
            }
            return new StructType(evenArgs);
        }
    }

    public static class ElementAtDeducer implements TypeDeducer {
        @Override
        public Type deduce(Type[] args) {
            // todo(xy)
            return null;
        }
    }
}
