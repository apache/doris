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
import org.apache.arrow.memory.RootAllocator;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/**
 * The resources of each generation: a recording allocator and a {@link FakeLanceNamespace}, and the Session a
 * test supplies. Tests whose calls never open a dataset supply none, since only an opened dataset uses it; native
 * tests supply real ones and keep them to inspect.
 */
final class FakeResourceFactory implements LanceCatalogClient.ResourceFactory {
    final List<RecordingAllocator> allocators = new CopyOnWriteArrayList<>();
    final List<FakeLanceNamespace> namespaces = new CopyOnWriteArrayList<>();
    final List<Session> sessions = new CopyOnWriteArrayList<>();
    /** The namespace the next generation connects to. */
    volatile Supplier<FakeLanceNamespace> nextNamespace = FakeLanceNamespace::new;
    /** The Session of the next generation; none by default. */
    volatile Supplier<Session> nextSession = () -> null;
    /** Counted down after every child allocator a read takes. */
    volatile CountDownLatch childAllocators = new CountDownLatch(0);

    @Override
    public BufferAllocator newAllocator(long limit) {
        RecordingAllocator allocator = new RecordingAllocator(limit, this);
        allocators.add(allocator);
        return allocator;
    }

    @Override
    public LanceNamespace connect(String implementation, Map<String, String> properties,
            BufferAllocator allocator) {
        FakeLanceNamespace namespace = nextNamespace.get();
        namespaces.add(namespace);
        return namespace;
    }

    @Override
    public Session newSession() {
        Session session = nextSession.get();
        if (session != null) {
            sessions.add(session);
        }
        return session;
    }

    /** Records that it was closed, and each child it hands out. */
    static final class RecordingAllocator extends RootAllocator {
        private final FakeResourceFactory factory;
        volatile boolean closed;

        private RecordingAllocator(long limit, FakeResourceFactory factory) {
            super(limit);
            this.factory = factory;
        }

        @Override
        public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
            BufferAllocator child = super.newChildAllocator(name, initReservation, maxAllocation);
            factory.childAllocators.countDown();
            return child;
        }

        @Override
        public void close() {
            closed = true;
            super.close();
        }
    }
}
