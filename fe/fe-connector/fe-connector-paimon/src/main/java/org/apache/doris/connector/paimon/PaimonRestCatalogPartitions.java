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

package org.apache.doris.connector.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.table.Table;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Supplier;

/** Preserves REST-owned partition visibility while retaining Doris' effective fallback table. */
final class PaimonRestCatalogPartitions {
    private PaimonRestCatalogPartitions() {
    }

    static List<Partition> listPartitions(
            RESTCatalog catalog, Identifier identifier, Table effectiveTable)
            throws Catalog.TableNotExistException {
        return listPartitions(restApi(catalog)::listPartitions, identifier,
                () -> CatalogUtils.listPartitionsFromFileSystem(effectiveTable));
    }

    static List<Partition> listPartitions(
            PartitionApi api, Identifier identifier, Supplier<List<Partition>> filesystemFallback)
            throws Catalog.TableNotExistException {
        try {
            return api.listPartitions(identifier);
        } catch (NoSuchResourceException e) {
            throw new Catalog.TableNotExistException(identifier, e);
        } catch (ForbiddenException e) {
            throw new Catalog.TableNoPermissionException(identifier, e);
        } catch (NotImplementedException e) {
            // Only the unsupported-endpoint branch may enumerate manifests; a successful REST
            // response is catalog-authoritative even when it differs from filesystem visibility.
            return filesystemFallback.get();
        }
    }

    private static RESTApi restApi(RESTCatalog catalog) {
        try {
            Method accessor = RESTCatalog.class.getDeclaredMethod("api");
            // Paimon keeps this accessor package-private even though preserving its native result
            // requires distinguishing it from RESTCatalog's raw-table filesystem fallback.
            accessor.setAccessible(true);
            return (RESTApi) accessor.invoke(catalog);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to access the Paimon REST partition API.", e);
        }
    }

    @FunctionalInterface
    interface PartitionApi {
        List<Partition> listPartitions(Identifier identifier);
    }
}
