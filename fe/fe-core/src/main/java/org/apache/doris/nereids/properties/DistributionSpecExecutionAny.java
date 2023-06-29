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

/**
 * Data can be in any instance, used in PhysicalDistribute.
 * Because all candidates in group could save as DistributionSpecAny's value in LowestCostPlan map
 * to distinguish DistributionSpecAny, we need a new Spec to represent must shuffle require.
 */
public class DistributionSpecExecutionAny extends DistributionSpec {

    public static final DistributionSpecExecutionAny INSTANCE = new DistributionSpecExecutionAny();

    private DistributionSpecExecutionAny() {
        super();
    }

    @Override
    public boolean satisfy(DistributionSpec other) {
        return other instanceof DistributionSpecAny || other instanceof DistributionSpecExecutionAny;
    }
}
