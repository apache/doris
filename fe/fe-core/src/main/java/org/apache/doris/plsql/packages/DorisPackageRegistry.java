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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/packages/HmsPackageRegistry.java
// and modified by Doris

package org.apache.doris.plsql.packages;

import org.apache.doris.plsql.metastore.PlsqlMetaClient;
import org.apache.doris.plsql.metastore.PlsqlPackage;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;

import java.util.Optional;

public class DorisPackageRegistry implements PackageRegistry {
    private final PlsqlMetaClient client;

    public DorisPackageRegistry(PlsqlMetaClient client) {
        this.client = client;
    }

    @Override
    public Optional<String> getPackage(String name) {
        try {
            PlsqlPackage pkg = findPackage(name);
            return pkg == null
                    ? Optional.empty()
                    : Optional.of(pkg.getHeader() + ";\n" + pkg.getBody());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createPackageHeader(String name, String header, boolean replace) {
        try {
            PlsqlPackage existing = findPackage(name);
            if (existing != null && !replace) {
                throw new RuntimeException("Package " + name + " already exists");
            }
            savePackage(name, header, "");
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createPackageBody(String name, String body, boolean replace) {
        try {
            PlsqlPackage existing = findPackage(name);
            if (existing == null || StringUtils.isEmpty(existing.getHeader())) {
                throw new RuntimeException("Package header does not exists " + name);
            }
            if (StringUtils.isNotEmpty(existing.getBody()) && !replace) {
                throw new RuntimeException("Package body " + name + " already exists");
            }
            savePackage(name, existing.getHeader(), body);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    private PlsqlPackage findPackage(String name) throws TException {
        return client.getPlsqlPackage(name.toUpperCase(), ConnectContext.get().getCurrentCatalog().getId(),
                ConnectContext.get().getCurrentDbId());
    }

    @Override
    public void dropPackage(String name) {
        client.dropPlsqlPackage(name, ConnectContext.get().getCurrentCatalog().getId(),
                ConnectContext.get().getCurrentDbId());
    }

    private void savePackage(String name, String header, String body) {
        client.addPlsqlPackage(name.toUpperCase(), ConnectContext.get().getCurrentCatalog().getId(),
                ConnectContext.get().getCurrentDbId(), ConnectContext.get().getQualifiedUser(), header, body);
    }
}
