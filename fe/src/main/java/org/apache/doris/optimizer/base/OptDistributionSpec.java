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

package org.apache.doris.optimizer.base;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.operator.OptExpressionHandle;
import org.apache.doris.optimizer.operator.OptPhysicalDistribution;

import java.util.List;

public class OptDistributionSpec implements OptPropertySpec {

    private final DistributionType type;
    private final OptHashDistributionItem hashItem;
    private final int singleItemBackendId;
    private OptDistributionSpec otherHashJoinDistributionSpec;

    private OptDistributionSpec(DistributionType type, OptHashDistributionItem item) {
        this.type = type;
        this.hashItem = item;
        this.singleItemBackendId = -1;
    }

    public OptHashDistributionItem getHashItem() {
        return hashItem;
    }

    public static OptDistributionSpec createSingleDistributionSpec() {
        return new OptDistributionSpec(DistributionType.SINGLE, null);
    }

    public static OptDistributionSpec createHashDistributionSpec(OptHashDistributionItem item) {
        return new OptDistributionSpec(DistributionType.HASH, item);
    }

    public static OptDistributionSpec createRandomDistributionSpec() {
        return new OptDistributionSpec(DistributionType.RANDOM, null);
    }

    public static OptDistributionSpec createAnyDistributionSpec() {
        return new OptDistributionSpec(DistributionType.ANY, null);
    }

    @Override
    public void appendEnforcers(RequiredPhysicalProperty reqdProp, OptExpression child,
                                OptExpressionHandle exprHandle, List<OptExpression> expressions) {
            final OptExpression enforcer = OptExpression.create(
                    new OptPhysicalDistribution(reqdProp.getDistributionProperty().getPropertySpec()), expressions);
            expressions.add(enforcer);
    }

    @Override
    public boolean isSatisfy(OptPropertySpec spec) {
        Preconditions.checkState(spec instanceof OptDistributionSpec,
                "The compare spec is not OptDistributionSpec.");
        final OptDistributionSpec distributionSpec = (OptDistributionSpec) spec;
        if (distributionSpec.type == DistributionType.ANY) {
            return true;
        }
        switch (type) {
            case HASH:
                return isHashSatisfy(distributionSpec);
            case RANDOM:
                return isRandomSatisfy(distributionSpec);
            case SINGLE:
                return isSingleSatisfySingle(distributionSpec);
            case Broadcast:
                return isReplicateSatisfy(distributionSpec);
                default:
                    Preconditions.checkState(false, "Unkown distribution type.");
        }
        return false;
    }

    private boolean isHashSatisfy(OptDistributionSpec spec) {
        if (otherHashJoinDistributionSpec != null && otherHashJoinDistributionSpec.isSatisfy(spec)) {
            return true;
        }

        if (spec.type == DistributionType.Broadcast) {
            return true;
        }

        if (spec.type != DistributionType.HASH) {
            return false;
        }

        return hashItem.isSatisfy(spec.hashItem);
    }

    public boolean isSingleSatisfySingle(OptDistributionSpec spec) {
        return spec.type == type && spec.singleItemBackendId == singleItemBackendId;
    }

    private boolean isRandomSatisfy(OptDistributionSpec spec) {
        return spec.type == type;
    }

    private boolean isReplicateSatisfy(OptDistributionSpec spec) {
        return spec.type == DistributionType.HASH;
    }

    public boolean isAny() {
        return type == DistributionType.ANY;
    }

    public boolean isSingle() {
        return type == DistributionType.SINGLE;
    }

    public boolean isHash() {
        return type == DistributionType.HASH;
    }

    public boolean isBroadcast() {
        return type == DistributionType.Broadcast;
    }

    @Override
    public int hashCode() {
        int hashCode = type.hashCode();
        if (hashItem != null) {
            hashCode = OptUtils.combineHash(hashCode, hashItem);
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || !(obj instanceof OptDistributionSpec)) {
            return false;
        }

        final OptDistributionSpec spec = (OptDistributionSpec) obj;
        return this.type == spec.type && this.hashItem == spec.hashItem;
    }

    public enum DistributionType {
        HASH,
        SINGLE,
        Broadcast,
        RANDOM,
        ANY,
    }
}
