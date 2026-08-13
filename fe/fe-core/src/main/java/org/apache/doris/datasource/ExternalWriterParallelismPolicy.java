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

package org.apache.doris.datasource;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import java.util.OptionalLong;

/** Plans external writer count without changing ownership-key semantics at runtime. */
public final class ExternalWriterParallelismPolicy {
    private ExternalWriterParallelismPolicy() {
    }

    /**
     * Plan writer parallelism. The scheduler may use fewer writers when the upstream fragment has
     * less capacity, but must never exceed this result.
     */
    public static ExternalWriterParallelism plan(ExternalWriteDistributionPlan distribution,
            Statistics statistics, int clusterWriterCapacity) {
        if (clusterWriterCapacity <= 0) {
            throw new IllegalArgumentException("writer capacity must be positive");
        }

        OptionalLong estimatedOwnershipCount;
        long planned;
        if (distribution.isSingleWriter()) {
            estimatedOwnershipCount = OptionalLong.of(1);
            planned = 1;
        } else if (distribution.isRandom()) {
            estimatedOwnershipCount = OptionalLong.empty();
            planned = clusterWriterCapacity;
        } else {
            estimatedOwnershipCount = estimateOwnershipCount(distribution, statistics);
            // Adaptive hash starts with one writer per key but may fan a hot key out to more
            // writers. Keep the full fragment capacity available for that runtime decision.
            planned = distribution.isAdaptiveHash() || !estimatedOwnershipCount.isPresent()
                    ? clusterWriterCapacity
                    : Math.min(clusterWriterCapacity, estimatedOwnershipCount.getAsLong());
        }
        String fallbackReason = distribution.getFallbackReason().orElse(null);
        return new ExternalWriterParallelism((int) Math.max(1, planned),
                estimatedOwnershipCount.isPresent() ? estimatedOwnershipCount.getAsLong() : null,
                fallbackReason);
    }

    private static OptionalLong estimateOwnershipCount(
            ExternalWriteDistributionPlan distribution, Statistics statistics) {
        OptionalLong routingCap = estimateRoutingCardinalityCap(distribution);
        if (statistics == null || !Double.isFinite(statistics.getRowCount())
                || statistics.getRowCount() < 0) {
            return routingCap;
        }
        if (statistics.getRowCount() == 0) {
            return OptionalLong.of(1);
        }
        long rowCount = saturatedCeil(statistics.getRowCount());
        long ownershipCount = 1;
        for (NamedExpression route : distribution.getRoutingExpressions()) {
            long routeCount = estimateExpressionCardinality(route, statistics, rowCount);
            long routeCap = distribution.getRoutingCardinalityCap(route.getExprId())
                    .orElse(Long.MAX_VALUE);
            ownershipCount = saturatedMultiply(
                    ownershipCount, Math.min(routeCount, routeCap), rowCount);
        }
        return OptionalLong.of(Math.max(1, Math.min(rowCount, ownershipCount)));
    }

    private static OptionalLong estimateRoutingCardinalityCap(
            ExternalWriteDistributionPlan distribution) {
        long cap = 1;
        for (NamedExpression route : distribution.getRoutingExpressions()) {
            OptionalLong routeCap = distribution.getRoutingCardinalityCap(route.getExprId());
            if (!routeCap.isPresent()) {
                return OptionalLong.empty();
            }
            cap = saturatedMultiply(cap, routeCap.getAsLong(), Long.MAX_VALUE);
        }
        return OptionalLong.of(Math.max(1, cap));
    }

    private static long estimateExpressionCardinality(
            Expression expression, Statistics statistics, long rowCount) {
        long cardinality = 1;
        boolean foundKnownInput = false;
        for (Slot input : expression.getInputSlots()) {
            ColumnStatistic columnStatistic = statistics.findColumnStatistics(input);
            if (columnStatistic == null || columnStatistic.isUnKnown
                    || !Double.isFinite(columnStatistic.ndv) || columnStatistic.ndv <= 0) {
                // A composite route can be as cardinal as its unknown input. Using only the
                // remaining known NDVs would turn incomplete statistics into an unsafe writer
                // cap, so retain the row-count upper bound (and any connector-provided cap).
                return rowCount;
            }
            foundKnownInput = true;
            cardinality = saturatedMultiply(
                    cardinality, saturatedCeil(columnStatistic.ndv), rowCount);
        }
        return foundKnownInput ? Math.max(1, cardinality) : rowCount;
    }

    private static long saturatedCeil(double value) {
        return value >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(1, (long) Math.ceil(value));
    }

    private static long saturatedMultiply(long left, long right, long limit) {
        if (left >= limit || right >= limit || left > limit / right) {
            return limit;
        }
        return Math.min(limit, left * right);
    }
}
