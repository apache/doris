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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Helpers for statement-scoped scan-reuse key construction.
 */
public final class ConnectorScanKeyUtils {

    private ConnectorScanKeyUtils() {
    }

    /**
     * Flatten nested AND conjuncts into an immutable list. The returned list preserves
     * the original conjunct order — callers that need order-independent matching must
     * sort the result themselves before comparison. Each conjunct is compared by
     * structural {@code equals} rather than by {@code toString}.
     */
    public static List<ConnectorExpression> canonicalFilterConjuncts(
            Optional<ConnectorExpression> filter) {
        if (filter.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorExpression> conjuncts = new ArrayList<>();
        flattenConjuncts(filter.get(), conjuncts);
        return Collections.unmodifiableList(conjuncts);
    }

    private static void flattenConjuncts(
            ConnectorExpression expr, List<ConnectorExpression> out) {
        if (expr instanceof ConnectorAnd) {
            for (ConnectorExpression conjunct : ((ConnectorAnd) expr).getConjuncts()) {
                flattenConjuncts(conjunct, out);
            }
        } else {
            out.add(expr);
        }
    }
}
