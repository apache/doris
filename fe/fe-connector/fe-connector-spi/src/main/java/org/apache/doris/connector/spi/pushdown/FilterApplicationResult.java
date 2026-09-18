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

package org.apache.doris.connector.spi.pushdown;

import java.util.Objects;

/**
 * Result of applying a filter to a table handle.
 *
 * @param <T> the table handle type
 */
public final class FilterApplicationResult<T> {

    private final T handle;
    private final ConnectorExpression remainingFilter;
    private final boolean precalculateStatistics;

    public FilterApplicationResult(T handle,
            ConnectorExpression remainingFilter,
            boolean precalculateStatistics) {
        this.handle = Objects.requireNonNull(handle, "handle");
        this.remainingFilter = remainingFilter;
        this.precalculateStatistics = precalculateStatistics;
    }

    public T getHandle() {
        return handle;
    }

    /**
     * Returns the part of the filter the connector did NOT consume, or {@code null} to claim it consumed
     * everything.
     *
     * <p><b>The engine only acts on {@code null}.</b> {@code PluginDrivenScanNode.convertPredicate} clears
     * every conjunct when this is {@code null}, and removes NOTHING when it is non-null — matching residual
     * sub-expressions back to individual conjuncts is not implemented, so returning "the half I could not
     * push" earns no credit for the half that was pushed. The connector-side consequence is asymmetric:</p>
     * <ul>
     *   <li>non-{@code null} (what all shipped connectors return, usually the original expression): BE
     *       re-evaluates the predicate, so a slightly WIDER pushdown is still correct.</li>
     *   <li>{@code null}: nothing re-checks the rows. Claim it only when every conjunct was translated
     *       EXACTLY — a wide pushdown now returns extra rows, a narrow one still loses them (see the package
     *       javadoc, Rule 1 and Rule 5).</li>
     * </ul>
     *
     * <p>Per-conjunct credit is available, but through the other protocol:
     * {@code ConnectorScanPlanProvider.getScanNodePropertiesResult} reporting not-pushed conjunct indices.</p>
     */
    public ConnectorExpression getRemainingFilter() {
        return remainingFilter;
    }

    public boolean isPrecalculateStatistics() {
        return precalculateStatistics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FilterApplicationResult)) {
            return false;
        }
        FilterApplicationResult<?> that = (FilterApplicationResult<?>) o;
        return precalculateStatistics == that.precalculateStatistics
                && handle.equals(that.handle)
                && Objects.equals(
                        remainingFilter, that.remainingFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                handle, remainingFilter, precalculateStatistics);
    }

    @Override
    public String toString() {
        return "FilterApplicationResult{handle=" + handle
                + ", precalculateStatistics="
                + precalculateStatistics + "}";
    }
}
