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

import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** RequireProperties */
public class RequireProperties {
    private final boolean followParentProperties;
    private final List<PhysicalProperties> properties;

    private RequireProperties(PhysicalProperties... properties) {
        this(false, properties);
    }

    private RequireProperties(boolean followParentProperties, PhysicalProperties... requireProperties) {
        Preconditions.checkArgument((followParentProperties == false && requireProperties.length > 0)
                || (followParentProperties == true && requireProperties.length == 0));
        this.properties = ImmutableList.copyOf(requireProperties);
        this.followParentProperties = followParentProperties;
    }

    public static RequireProperties of(PhysicalProperties... properties) {
        return new RequireProperties(properties);
    }

    public static RequireProperties followParent() {
        return new RequireProperties(true);
    }

    public RequirePropertiesTree withChildren(RequireProperties... requireProperties) {
        List<RequirePropertiesTree> children = Arrays.stream(requireProperties)
                .map(child -> new RequirePropertiesTree(child, ImmutableList.of()))
                .collect(ImmutableList.toImmutableList());
        return new RequirePropertiesTree(this, children);
    }

    public RequirePropertiesTree withChildren(RequirePropertiesTree... children) {
        return new RequirePropertiesTree(this, ImmutableList.copyOf(children));
    }

    public boolean isFollowParentProperties() {
        return followParentProperties;
    }

    public List<PhysicalProperties> getProperties() {
        return properties;
    }

    /** computeRequirePhysicalProperties */
    public List<PhysicalProperties> computeRequirePhysicalProperties(
            Plan currentPlan, PhysicalProperties parentRequire) {
        int childNum = currentPlan.arity();
        if (followParentProperties) {
            // CostAndEnforcerJob will modify this list: requestChildrenProperties.set(curChildIndex, outputProperties)
            List<PhysicalProperties> requireProperties = Lists.newArrayListWithCapacity(childNum);
            for (int i = 0; i < childNum; i++) {
                requireProperties.add(parentRequire);
            }
            return requireProperties;
        } else {
            Preconditions.checkState(properties.size() == childNum,
                    "Expect require physical properties num is " + childNum + ", but real is "
                            + properties.size());
            // CostAndEnforcerJob will modify this list: requestChildrenProperties.set(curChildIndex, outputProperties)
            return Lists.newArrayList(properties);
        }
    }

    @Override
    public String toString() {
        if (followParentProperties) {
            return "followParentProperties";
        } else {
            return properties.toString();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RequireProperties that = (RequireProperties) o;
        return followParentProperties == that.followParentProperties
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(followParentProperties, properties);
    }

    /** RequirePropertiesTree */
    public static class RequirePropertiesTree {
        public final RequireProperties requireProperties;
        public final List<RequirePropertiesTree> children;

        private RequirePropertiesTree(RequireProperties requireProperties, List<RequirePropertiesTree> children) {
            this.requireProperties = requireProperties;
            this.children = children;
        }
    }
}
