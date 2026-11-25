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

package org.apache.doris.tablefunction;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.spi.search.ConnectorSearchFunction;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** {@code vector_search(...)}: the rows of a plugin catalog's table nearest to a query vector. */
public class VectorSearchTableValuedFunction extends ExternalSearchTableValuedFunction {
    public static final String NAME = "vector_search";

    public VectorSearchTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        this(properties, Collections.emptySet());
    }

    public VectorSearchTableValuedFunction(Map<String, String> properties, Set<String> deferredArguments)
            throws AnalysisException {
        super(ConnectorSearchFunction.VECTOR_SEARCH, properties, deferredArguments);
    }
}
