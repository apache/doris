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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Physical properties used in cascades. If upstream mismatches downstream, a PhysicalDistribute will be generated.
 */
public class PhysicalProperties {

    public static PhysicalProperties ANY = new PhysicalProperties();

    public static PhysicalProperties STORAGE_ANY = new PhysicalProperties(DistributionSpecStorageAny.INSTANCE);

    public static PhysicalProperties EXECUTION_ANY = new PhysicalProperties(DistributionSpecExecutionAny.INSTANCE);

    public static PhysicalProperties REPLICATED = new PhysicalProperties(DistributionSpecReplicated.INSTANCE);

    public static PhysicalProperties GATHER = new PhysicalProperties(DistributionSpecGather.INSTANCE);

    public static PhysicalProperties STORAGE_GATHER = new PhysicalProperties(DistributionSpecStorageGather.INSTANCE);

    public static PhysicalProperties MUST_SHUFFLE = new PhysicalProperties(DistributionSpecMustShuffle.INSTANCE);

    public static PhysicalProperties TABLET_ID_SHUFFLE
            = new PhysicalProperties(DistributionSpecOlapTableSinkHashPartitioned.INSTANCE);

    public static PhysicalProperties SINK_RANDOM_PARTITIONED
            = new PhysicalProperties(DistributionSpecHiveTableSinkUnPartitioned.INSTANCE);

    // gather then broadcast to all BE with exact one instance
    public static PhysicalProperties ALL_SINGLETON = new PhysicalProperties(DistributionSpecAllSingleton.INSTANCE);

    private final OrderSpec orderSpec;

    private final DistributionSpec distributionSpec;

    private final Optional<NaturalDistributionMappingSpec> naturalDistributionMappingSpec;

    private Integer hashCode = null;

    private PhysicalProperties() {
        this(DistributionSpecAny.INSTANCE, new OrderSpec(), Optional.empty());
    }

    public PhysicalProperties(DistributionSpec distributionSpec) {
        this(distributionSpec, new OrderSpec(), naturalMappingSpecFrom(distributionSpec));
    }

    public PhysicalProperties(OrderSpec orderSpec) {
        this(DistributionSpecAny.INSTANCE, orderSpec, Optional.empty());
    }

    public PhysicalProperties(DistributionSpec distributionSpec, OrderSpec orderSpec) {
        this(distributionSpec, orderSpec, naturalMappingSpecFrom(distributionSpec));
    }

    /** Constructor with mapping-based natural bucket locality. */
    public PhysicalProperties(DistributionSpec distributionSpec, OrderSpec orderSpec,
            Optional<NaturalDistributionMappingSpec> naturalDistributionMappingSpec) {
        this.distributionSpec = distributionSpec;
        this.orderSpec = orderSpec;
        this.naturalDistributionMappingSpec = naturalDistributionMappingSpec;
    }

    private static Optional<NaturalDistributionMappingSpec> naturalMappingSpecFrom(
            DistributionSpec distributionSpec) {
        return distributionSpec instanceof DistributionSpecHash
                ? NaturalDistributionMappingSpec.fromHashSpec((DistributionSpecHash) distributionSpec)
                : Optional.empty();
    }

    /**
     * create hash info from orderedShuffledColumns, ignore non slot reference expression.
     */
    public static PhysicalProperties createHash(
            Collection<? extends Expression> orderedShuffledColumns, ShuffleType shuffleType) {
        List<ExprId> partitionedSlots = orderedShuffledColumns.stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .map(SlotReference::getExprId)
                .collect(Collectors.toList());
        return partitionedSlots.isEmpty() ? PhysicalProperties.GATHER : createHash(partitionedSlots, shuffleType);
    }

    public static PhysicalProperties createHash(List<ExprId> orderedShuffledColumns, ShuffleType shuffleType) {
        return orderedShuffledColumns.isEmpty()
                ? PhysicalProperties.GATHER
                : new PhysicalProperties(new DistributionSpecHash(orderedShuffledColumns, shuffleType));
    }

    public static PhysicalProperties createHash(DistributionSpecHash distributionSpecHash) {
        return new PhysicalProperties(distributionSpecHash);
    }

    /** createAnyFromHash */
    public static PhysicalProperties createAnyFromHash(DistributionSpecHash... childSpecs) {
        for (DistributionSpecHash childSpec : childSpecs) {
            if (childSpec.getShuffleType() == ShuffleType.NATURAL) {
                return PhysicalProperties.STORAGE_ANY;
            }
        }
        return PhysicalProperties.ANY;
    }

    public PhysicalProperties withOrderSpec(OrderSpec orderSpec) {
        return new PhysicalProperties(distributionSpec, orderSpec, naturalDistributionMappingSpec);
    }

    public PhysicalProperties withNaturalDistributionMappingSpec(
            Optional<NaturalDistributionMappingSpec> naturalDistributionMappingSpec) {
        return new PhysicalProperties(distributionSpec, orderSpec, naturalDistributionMappingSpec);
    }

    /** Return whether the current properties satisfy the required properties. */
    public boolean satisfy(PhysicalProperties other) {
        if (!orderSpec.satisfy(other.orderSpec)) {
            return false;
        }
        if (other.distributionSpec instanceof DistributionSpecHash
                && ((DistributionSpecHash) other.distributionSpec).getShuffleType()
                        == ShuffleType.COLOCATE_MAPPING_REQUIRE) {
            return naturalDistributionMappingSpec
                    .map(spec -> spec.satisfy(
                            ((DistributionSpecHash) other.distributionSpec).getOrderedShuffledColumns()))
                    .orElse(false);
        }
        return distributionSpec.satisfy(other.distributionSpec);
    }

    public OrderSpec getOrderSpec() {
        return orderSpec;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    public Optional<NaturalDistributionMappingSpec> getNaturalDistributionMappingSpec() {
        return naturalDistributionMappingSpec;
    }

    /** Whether a missing property can be produced by adding physical enforcers. */
    public boolean isEnforceable() {
        return !(distributionSpec instanceof DistributionSpecHash)
                || ((DistributionSpecHash) distributionSpec).getShuffleType()
                        != ShuffleType.COLOCATE_MAPPING_REQUIRE;
    }

    public boolean isDistributionOnlyProperties() {
        return orderSpec.getOrderKeys().isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalProperties that = (PhysicalProperties) o;
        if (this.hashCode() != that.hashCode()) {
            return false;
        }
        return orderSpec.equals(that.orderSpec)
                && distributionSpec.equals(that.distributionSpec)
                && naturalDistributionMappingSpec.equals(that.naturalDistributionMappingSpec);
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = Objects.hash(orderSpec, distributionSpec, naturalDistributionMappingSpec);
        }
        return hashCode;
    }

    @Override
    public String toString() {
        if (this.equals(ANY)) {
            return "ANY";
        }
        if (this.equals(REPLICATED)) {
            return "REPLICATED";
        }
        if (this.equals(GATHER)) {
            return "GATHER";
        }
        return distributionSpec.toString() + " " + orderSpec.toString()
                + naturalDistributionMappingSpec.map(spec -> " " + spec).orElse("");
    }

}
