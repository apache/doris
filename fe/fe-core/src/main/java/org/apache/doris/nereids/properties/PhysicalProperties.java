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

import java.util.Objects;

/**
 * Physical properties used in cascades.
 */
public class PhysicalProperties {

    public static PhysicalProperties ANY = new PhysicalProperties();

    public static PhysicalProperties REPLICATED = new PhysicalProperties(DistributionSpecReplicated.INSTANCE);

    public static PhysicalProperties GATHER = new PhysicalProperties(DistributionSpecGather.INSTANCE);

    private final OrderSpec orderSpec;

    private final DistributionSpec distributionSpec;

    private Integer hashCode = null;

    private PhysicalProperties() {
        this.orderSpec = new OrderSpec();
        this.distributionSpec = DistributionSpecAny.INSTANCE;
    }

    public PhysicalProperties(DistributionSpec distributionSpec) {
        this.distributionSpec = distributionSpec;
        this.orderSpec = new OrderSpec();
    }

    public PhysicalProperties(OrderSpec orderSpec) {
        this.orderSpec = orderSpec;
        this.distributionSpec = DistributionSpecAny.INSTANCE;
    }

    public PhysicalProperties(DistributionSpec distributionSpec, OrderSpec orderSpec) {
        this.distributionSpec = distributionSpec;
        this.orderSpec = orderSpec;
    }

    public static PhysicalProperties createHash(DistributionSpecHash distributionSpecHash) {
        return new PhysicalProperties(distributionSpecHash);
    }

    // Current properties satisfies other properties.
    public boolean satisfy(PhysicalProperties other) {
        return orderSpec.satisfy(other.orderSpec) && distributionSpec.satisfy(other.distributionSpec);
    }

    public OrderSpec getOrderSpec() {
        return orderSpec;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
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
                && distributionSpec.equals(that.distributionSpec);
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = Objects.hash(orderSpec, distributionSpec);
        }
        return hashCode;
    }

    @Override
    public String toString() {
        return distributionSpec.toString() + " " + orderSpec.toString();
    }

}
