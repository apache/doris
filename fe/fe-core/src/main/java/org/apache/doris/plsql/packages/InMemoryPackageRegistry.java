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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/packages/InMemoryPackageRegistry.java
// and modified by Doris

package org.apache.doris.plsql.packages;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InMemoryPackageRegistry implements PackageRegistry {
    private Map<String, Source> registry = new HashMap<>();

    @Override
    public Optional<String> getPackage(String name) {
        Source src = registry.get(name.toUpperCase());
        return src == null
                ? Optional.empty()
                : Optional.of(src.header + ";\n" + src.body);
    }

    @Override
    public void createPackageHeader(String name, String header, boolean replace) {
        if (registry.containsKey(name) && !replace) {
            throw new RuntimeException("Package " + name + " already exits");
        }
        registry.put(name, new Source(header, ""));
    }

    @Override
    public void createPackageBody(String name, String body, boolean replace) {
        Source existing = registry.get(name);
        if (existing == null || StringUtils.isEmpty(existing.header)) {
            throw new RuntimeException("Package header does not exists " + name);
        }
        if (StringUtils.isNotEmpty(existing.body) && !replace) {
            throw new RuntimeException("Package body " + name + " already exits");
        }
        registry.getOrDefault(name, new Source("", "")).body = body;
    }

    @Override
    public void dropPackage(String name) {
        registry.remove(name);
    }

    private static class Source {
        String header;
        String body;

        public Source(String header, String body) {
            this.header = header;
            this.body = body;
        }
    }
}
