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

package org.apache.doris.plsql.executor;

import org.apache.doris.qe.Coordinator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Closeable;
import java.util.Collections;

class DorisRowResultTest {

    @Test
    void closeReleasesCoordinatorAndDetachedStatementResourcesOnce() throws Exception {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Closeable statementResources = Mockito.mock(Closeable.class);
        DorisRowResult result = new DorisRowResult(
                coordinator, Collections.emptyList(), Collections.emptyList(), statementResources);

        result.close();
        result.close();

        Mockito.verify(coordinator).close();
        Mockito.verify(statementResources).close();
    }

    @Test
    void coordinatorFailureDoesNotSkipStatementResourceCleanup() throws Exception {
        Coordinator coordinator = Mockito.mock(Coordinator.class);
        Closeable statementResources = Mockito.mock(Closeable.class);
        Mockito.doThrow(new IllegalStateException("coordinator close failed")).when(coordinator).close();
        DorisRowResult result = new DorisRowResult(
                coordinator, Collections.emptyList(), Collections.emptyList(), statementResources);

        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class, result::close);

        Assertions.assertEquals("coordinator close failed", failure.getMessage());
        Mockito.verify(statementResources).close();
    }

    @Test
    void noCoordinatorClosesDetachedStatementResourcesOnFirstFetch() throws Exception {
        Closeable statementResources = Mockito.mock(Closeable.class);
        DorisRowResult result = new DorisRowResult(
                null, Collections.emptyList(), Collections.emptyList(), statementResources);

        Assertions.assertFalse(result.next());

        Mockito.verify(statementResources).close();
    }
}
