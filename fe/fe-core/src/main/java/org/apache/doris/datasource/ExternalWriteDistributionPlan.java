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

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/** Immutable routing plan produced by an external table connector. */
public final class ExternalWriteDistributionPlan {

    /** How rows are grouped across table writers. */
    public enum RouteKind {
        SINGLE_WRITER,
        RANDOM,
        STATELESS_HASH,
        ADAPTIVE_HASH
    }

    private final RouteKind routeKind;
    private final ImmutableList<NamedExpression> routingExpressions;
    private final ImmutableMap<ExprId, Long> routingCardinalityCaps;
    private final String fallbackReason;

    private ExternalWriteDistributionPlan(RouteKind routeKind,
            List<NamedExpression> routingExpressions,
            Map<ExprId, Long> routingCardinalityCaps, String fallbackReason) {
        this.routeKind = Objects.requireNonNull(routeKind, "routeKind");
        this.routingExpressions = ImmutableList.copyOf(
                Objects.requireNonNull(routingExpressions, "routingExpressions"));
        this.routingCardinalityCaps = ImmutableMap.copyOf(
                Objects.requireNonNull(routingCardinalityCaps, "routingCardinalityCaps"));
        this.fallbackReason = fallbackReason;
        if (requiresRoutingExpressions(routeKind) && routingExpressions.isEmpty()) {
            throw new IllegalArgumentException("Hash route requires routing expressions");
        }
        if (!requiresRoutingExpressions(routeKind) && !routingExpressions.isEmpty()) {
            throw new IllegalArgumentException(
                    "Only hash routes may contain routing expressions");
        }
        for (Map.Entry<ExprId, Long> entry : routingCardinalityCaps.entrySet()) {
            if (entry.getValue() <= 0 || routingExpressions.stream()
                    .noneMatch(expression -> expression.getExprId().equals(entry.getKey()))) {
                throw new IllegalArgumentException(
                        "Routing cardinality cap must reference a route and be positive");
            }
        }
    }

    /** Create a safe single-writer fallback. */
    public static ExternalWriteDistributionPlan singleWriter(String reason) {
        return new ExternalWriteDistributionPlan(
                RouteKind.SINGLE_WRITER, ImmutableList.of(), ImmutableMap.of(),
                Objects.requireNonNull(reason, "reason"));
    }

    /** Create a random route for formats without a writer ownership key. */
    public static ExternalWriteDistributionPlan random() {
        return new ExternalWriteDistributionPlan(
                RouteKind.RANDOM, ImmutableList.of(), ImmutableMap.of(), null);
    }

    /** Create a stateless hash route whose expressions become hidden child outputs. */
    public static ExternalWriteDistributionPlan statelessHash(
            List<NamedExpression> routingExpressions) {
        return new ExternalWriteDistributionPlan(
                RouteKind.STATELESS_HASH, routingExpressions, ImmutableMap.of(), null);
    }

    /** Create an adaptive hash route whose hot keys may be handled by multiple writers. */
    public static ExternalWriteDistributionPlan adaptiveHash(
            List<NamedExpression> routingExpressions,
            Map<ExprId, Long> routingCardinalityCaps) {
        return new ExternalWriteDistributionPlan(
                RouteKind.ADAPTIVE_HASH, routingExpressions, routingCardinalityCaps, null);
    }

    /** Create a hash route with connector-proven upper bounds for individual routing keys. */
    public static ExternalWriteDistributionPlan statelessHash(
            List<NamedExpression> routingExpressions,
            Map<ExprId, Long> routingCardinalityCaps) {
        return new ExternalWriteDistributionPlan(
                RouteKind.STATELESS_HASH, routingExpressions, routingCardinalityCaps, null);
    }

    public RouteKind getRouteKind() {
        return routeKind;
    }

    public List<NamedExpression> getRoutingExpressions() {
        return routingExpressions;
    }

    public List<ExprId> getRoutingExprIds() {
        return routingExpressions.stream()
                .map(NamedExpression::getExprId)
                .collect(Collectors.toList());
    }

    public OptionalLong getRoutingCardinalityCap(ExprId exprId) {
        Long cap = routingCardinalityCaps.get(exprId);
        return cap == null ? OptionalLong.empty() : OptionalLong.of(cap);
    }

    public Optional<String> getFallbackReason() {
        return Optional.ofNullable(fallbackReason);
    }

    public boolean isSingleWriter() {
        return routeKind == RouteKind.SINGLE_WRITER;
    }

    public boolean isRandom() {
        return routeKind == RouteKind.RANDOM;
    }

    public boolean isAdaptiveHash() {
        return routeKind == RouteKind.ADAPTIVE_HASH;
    }

    public boolean hasRoutingExpressions() {
        return requiresRoutingExpressions(routeKind);
    }

    private static boolean requiresRoutingExpressions(RouteKind routeKind) {
        return routeKind == RouteKind.STATELESS_HASH || routeKind == RouteKind.ADAPTIVE_HASH;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ExternalWriteDistributionPlan)) {
            return false;
        }
        ExternalWriteDistributionPlan that = (ExternalWriteDistributionPlan) other;
        return routeKind == that.routeKind
                && routingExpressions.equals(that.routingExpressions)
                && routingCardinalityCaps.equals(that.routingCardinalityCaps)
                && Objects.equals(fallbackReason, that.fallbackReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(routeKind, routingExpressions, routingCardinalityCaps, fallbackReason);
    }
}
