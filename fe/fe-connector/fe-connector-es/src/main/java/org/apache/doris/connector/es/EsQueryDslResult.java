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

package org.apache.doris.connector.es;

import java.util.List;

/**
 * Result of building an ES query DSL from connector expressions.
 * Contains both the JSON DSL string and the list of expression indices
 * that could not be pushed down to ES.
 */
public final class EsQueryDslResult {

    private final String queryDsl;
    private final List<Integer> notPushedIndices;

    public EsQueryDslResult(String queryDsl, List<Integer> notPushedIndices) {
        this.queryDsl = queryDsl;
        this.notPushedIndices = notPushedIndices;
    }

    /** The ES Query DSL JSON string. */
    public String getQueryDsl() {
        return queryDsl;
    }

    /**
     * Indices (0-based) of the top-level AND conjuncts that could NOT be
     * pushed down to ES. The caller should keep these for local evaluation.
     * An empty list means all conjuncts were pushed successfully.
     */
    public List<Integer> getNotPushedIndices() {
        return notPushedIndices;
    }
}
