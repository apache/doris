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

package org.apache.doris.connector.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.TableExistsRequest;
import org.lance.namespace.model.TableExistsResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A namespace that records the requests it is sent and answers them as the test scripts: every describe by
 * default as a plain filesystem table, every table as existing, every listing as empty. A script may block, to
 * hold a call in flight. Closing it is recorded, and can be made to fail.
 */
final class FakeLanceNamespace implements LanceNamespace, AutoCloseable {
    final List<DescribeTableRequest> describes = new CopyOnWriteArrayList<>();
    final List<TableExistsRequest> existenceChecks = new CopyOnWriteArrayList<>();
    final List<ListNamespacesRequest> namespaceListings = new CopyOnWriteArrayList<>();
    final AtomicInteger closes = new AtomicInteger();
    volatile Function<DescribeTableRequest, DescribeTableResponse> describe =
            request -> new DescribeTableResponse().tableUri("file:///warehouse/items.lance");
    volatile Consumer<TableExistsRequest> tableExists = request -> { };
    volatile Function<ListNamespacesRequest, ListNamespacesResponse> listNamespaces =
            request -> new ListNamespacesResponse();
    volatile RuntimeException closeFailure;

    /** Answers every describe with {@code response}. */
    FakeLanceNamespace describing(DescribeTableResponse response) {
        describe = request -> response;
        return this;
    }

    @Override
    public void initialize(Map<String, String> properties, BufferAllocator allocator) {
    }

    @Override
    public String namespaceId() {
        return "fake";
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request) {
        describes.add(request);
        return describe.apply(request);
    }

    @Override
    public TableExistsResponse tableExists(TableExistsRequest request) {
        existenceChecks.add(request);
        tableExists.accept(request);
        return new TableExistsResponse();
    }

    @Override
    public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
        namespaceListings.add(request);
        return listNamespaces.apply(request);
    }

    @Override
    public void close() {
        closes.incrementAndGet();
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
