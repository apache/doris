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

package org.apache.doris.nereids.trees.plans.distribute;

import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;

import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * FragmentIdMapping:
 * key: PlanFragmentId
 * value: T
 *
 * NOTE: this map should order by PlanFragmentId asc
 */
public class FragmentIdMapping<T> extends TreeMap<PlanFragmentId, T> {
    public FragmentIdMapping() {
    }

    public FragmentIdMapping(Comparator<? super PlanFragmentId> comparator) {
        super(comparator);
    }

    public FragmentIdMapping(Map<? extends PlanFragmentId, ? extends T> m) {
        super(m);
    }

    public FragmentIdMapping(SortedMap<PlanFragmentId, ? extends T> m) {
        super(m);
    }

    /** getByChildrenFragments */
    public List<T> getByChildrenFragments(PlanFragment fragment) {
        List<PlanFragment> children = fragment.getChildren();
        ImmutableList.Builder<T> values = ImmutableList.builderWithExpectedSize(children.size());
        for (PlanFragment child : children) {
            values.add(get(child.getFragmentId()));
        }
        return values.build();
    }

    public static FragmentIdMapping<PlanFragment> buildFragmentMapping(List<PlanFragment> fragments) {
        FragmentIdMapping<PlanFragment> idToFragments = new FragmentIdMapping<>();
        for (PlanFragment fragment : fragments) {
            idToFragments.put(fragment.getFragmentId(), fragment);
        }
        return idToFragments;
    }
}
