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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * One call of a search table function, as the engine hands it to the connector of the table it searches; see
 * {@link ConnectorSearchOps}.
 *
 * <p>The engine reads only the {@code table} argument, which names the table, and passes every other argument on
 * unread: what they mean, which are required and which values are valid is the connector's to decide.</p>
 */
public final class ConnectorSearchRequest {
    private final ConnectorSearchFunction function;
    private final Map<String, String> arguments;
    private final Set<String> deferredArguments;

    /**
     * @throws IllegalArgumentException when an argument is both given and deferred
     */
    public ConnectorSearchRequest(ConnectorSearchFunction function, Map<String, String> arguments,
            Set<String> deferredArguments) {
        this.function = Objects.requireNonNull(function, "function");
        this.arguments = Collections.unmodifiableMap(new LinkedHashMap<>(arguments));
        this.deferredArguments = Collections.unmodifiableSet(new LinkedHashSet<>(deferredArguments));
        if (!Collections.disjoint(this.arguments.keySet(), this.deferredArguments)) {
            throw new IllegalArgumentException("A search argument cannot be both given and deferred: "
                    + this.arguments.keySet() + " and " + this.deferredArguments);
        }
    }

    public ConnectorSearchFunction getFunction() {
        return function;
    }

    /**
     * The arguments other than {@code table}, by name. The names are in lower case ({@code Locale.ROOT}) and
     * unique; the values are as the user wrote them, untrimmed.
     */
    public Map<String, String> getArguments() {
        return arguments;
    }

    /**
     * The arguments whose values are not known yet, by their lower-case names; empty except while a statement is
     * prepared. A prepared statement can leave an argument to a parameter ({@code "top_k" = ?}) that is bound only
     * when the statement executes, and the engine prepares it to learn the columns. The connector returns the
     * columns without these values when it can, and otherwise rejects the parameter with a message saying the
     * argument must be a constant. Each execution prepares the search again with every value bound.
     */
    public Set<String> getDeferredArguments() {
        return deferredArguments;
    }

    /** Names the arguments but not their values: a query vector or a filter is long, and both are user data. */
    @Override
    public String toString() {
        return "ConnectorSearchRequest{function=" + function + ", arguments=" + arguments.keySet()
                + ", deferred=" + deferredArguments + "}";
    }
}
