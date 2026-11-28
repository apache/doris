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

package org.apache.doris.connector.spi.search;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectorSearchValuesTest {

    private static final ConnectorTableHandle HANDLE = new ConnectorTableHandle() {
    };

    @Test
    public void functionsFixTheirResultColumnAndMergeOrder() {
        Assertions.assertEquals("vector_search", ConnectorSearchFunction.VECTOR_SEARCH.getFunctionName());
        Assertions.assertEquals("_distance", ConnectorSearchFunction.VECTOR_SEARCH.getResultColumn());
        Assertions.assertTrue(ConnectorSearchFunction.VECTOR_SEARCH.isResultAscending());
        Assertions.assertEquals("full_text_search", ConnectorSearchFunction.FULL_TEXT_SEARCH.getFunctionName());
        Assertions.assertEquals("_score", ConnectorSearchFunction.FULL_TEXT_SEARCH.getResultColumn());
        Assertions.assertFalse(ConnectorSearchFunction.FULL_TEXT_SEARCH.isResultAscending());
    }

    @Test
    public void requestKeepsItsOwnCopyOfTheArguments() {
        Map<String, String> arguments = new HashMap<>();
        arguments.put("column", "embedding");
        Set<String> deferred = new HashSet<>();
        deferred.add("query_vector");
        ConnectorSearchRequest request =
                new ConnectorSearchRequest(ConnectorSearchFunction.VECTOR_SEARCH, arguments, deferred);
        arguments.put("top_k", "3");
        deferred.add("offset");

        Assertions.assertEquals(Collections.singletonMap("column", "embedding"), request.getArguments());
        Assertions.assertEquals(Collections.singleton("query_vector"), request.getDeferredArguments());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> request.getArguments().put("top_k", "3"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> request.getDeferredArguments().add("offset"));
    }

    @Test
    public void requestRefusesAnArgumentThatIsBothGivenAndDeferred() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorSearchRequest(ConnectorSearchFunction.VECTOR_SEARCH,
                        Collections.singletonMap("top_k", "3"), Collections.singleton("top_k")));
        Assertions.assertTrue(e.getMessage().contains("top_k"), e.getMessage());
    }

    @Test
    public void requestPrintsArgumentNamesWithoutTheirValues() {
        ConnectorSearchRequest request = new ConnectorSearchRequest(ConnectorSearchFunction.FULL_TEXT_SEARCH,
                Collections.singletonMap("query", "secret words"), Collections.emptySet());
        Assertions.assertTrue(request.toString().contains("query"), request.toString());
        Assertions.assertFalse(request.toString().contains("secret words"), request.toString());
    }

    @Test
    public void searchKeepsItsOwnCopyOfTheColumns() {
        List<ConnectorColumn> columns = new ArrayList<>();
        columns.add(new ConnectorColumn("_distance", ConnectorType.of("FLOAT"), null, false, null));
        ConnectorSearch search = new ConnectorSearch(HANDLE, columns, 10, 0, ConnectorSearchLazyMaterialization.NONE);
        columns.clear();

        Assertions.assertSame(HANDLE, search.getScanHandle());
        Assertions.assertEquals(1, search.getColumns().size());
        Assertions.assertEquals(10, search.getTopK());
        Assertions.assertEquals(0, search.getOffset());
        Assertions.assertEquals(ConnectorSearchLazyMaterialization.NONE, search.getLazyMaterialization());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> search.getColumns().clear());
    }

    @Test
    public void searchRefusesBoundsTheMergeCannotApply() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConnectorSearch(HANDLE,
                Collections.emptyList(), 0, 0, ConnectorSearchLazyMaterialization.NONE));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConnectorSearch(HANDLE,
                Collections.emptyList(), 1, -1, ConnectorSearchLazyMaterialization.NONE));
        ConnectorSearch skipping = new ConnectorSearch(HANDLE, Collections.emptyList(), 1, 5,
                ConnectorSearchLazyMaterialization.TOP_LEVEL_COLUMNS);
        Assertions.assertEquals(5, skipping.getOffset());
    }
}
