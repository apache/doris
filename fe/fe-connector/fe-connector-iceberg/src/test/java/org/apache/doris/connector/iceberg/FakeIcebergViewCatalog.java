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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link FakeIcebergCatalog} that also implements {@link ViewCatalog}, for exercising the view-filtering
 * branch of {@code listTableNames}. The view names returned by {@link #listViews} are subtracted from the
 * table list by the seam when view filtering is enabled.
 */
class FakeIcebergViewCatalog extends FakeIcebergCatalog implements ViewCatalog {

    /** namespace -> view names returned by listViews. */
    final Map<Namespace, List<String>> viewsByNs = new HashMap<>();

    @Override
    public String name() {
        return "fake-view-catalog";
    }

    // Catalog and ViewCatalog both declare a default initialize(String, Map); a class implementing both
    // must override it to resolve the diamond. Not exercised by the listing path -> fail loud.
    @Override
    public void initialize(String name, Map<String, String> properties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TableIdentifier> listViews(Namespace namespace) {
        log.add("listViews:" + namespace);
        return viewsByNs.getOrDefault(namespace, Collections.<String>emptyList()).stream()
                .map(n -> TableIdentifier.of(namespace, n))
                .collect(Collectors.toList());
    }

    @Override
    public boolean viewExists(TableIdentifier identifier) {
        log.add("viewExists:" + identifier);
        return viewsByNs.getOrDefault(identifier.namespace(), Collections.<String>emptyList())
                .contains(identifier.name());
    }

    @Override
    public View loadView(TableIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ViewBuilder buildView(TableIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropView(TableIdentifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameView(TableIdentifier from, TableIdentifier to) {
        throw new UnsupportedOperationException();
    }
}
