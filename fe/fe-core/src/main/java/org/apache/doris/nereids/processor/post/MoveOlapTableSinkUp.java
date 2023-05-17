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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;

import com.google.common.collect.ImmutableList;

/**
 * move olap table sink up
 */
public class MoveOlapTableSinkUp extends PlanPostProcessor {
    @Override
    public Plan visitPhysicalDistribute(PhysicalDistribute distribute, CascadesContext ctx) {
        if (distribute.child() instanceof PhysicalOlapTableSink) {
            PhysicalOlapTableSink sink = ((PhysicalOlapTableSink<?>) distribute.child());
            distribute = distribute.withChildren(sink.children());
            return sink.withChildren(ImmutableList.of(distribute));
        }
        return distribute;
    }
}
