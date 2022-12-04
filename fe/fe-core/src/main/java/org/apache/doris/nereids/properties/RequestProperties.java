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

import java.util.List;
import java.util.Objects;

/** RequestPhysicalProperties */
public class RequestProperties {
    private final boolean followParentProperties;
    private final List<PhysicalProperties> properties;

    private RequestProperties(PhysicalProperties... properties) {
        this(false, properties);
    }

    private RequestProperties(boolean followParentProperties, PhysicalProperties... requestProperties) {
        Preconditions.checkArgument((followParentProperties == false && requestProperties.length > 0)
                || (followParentProperties == true && requestProperties.length == 0));
        this.properties = ImmutableList.copyOf(requestProperties);
        this.followParentProperties = followParentProperties;
    }

    public static RequestProperties of(PhysicalProperties... properties) {
        return new RequestProperties(properties);
    }

    public static RequestProperties followParent() {
        return new RequestProperties(true);
    }

    /** computeRequestPhysicalProperties */
    public List<PhysicalProperties> computeRequestPhysicalProperties(
            Plan currentPlan, PhysicalProperties parentRequest) {
        int childNum = currentPlan.arity();
        if (followParentProperties) {
            // CostAndEnforcerJob will modify this list: requestChildrenProperties.set(curChildIndex, outputProperties)
            List<PhysicalProperties> requestProperties = Lists.newArrayListWithCapacity(childNum);
            for (int i = 0; i < childNum; i++) {
                requestProperties.add(parentRequest);
            }
            return requestProperties;
        } else {
            Preconditions.checkState(properties.size() == childNum,
                    "Expect request physical properties num is " + childNum + ", but real is "
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
        RequestProperties that = (RequestProperties) o;
        return followParentProperties == that.followParentProperties && Objects.equals(properties,
                that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(followParentProperties, properties);
    }
}
