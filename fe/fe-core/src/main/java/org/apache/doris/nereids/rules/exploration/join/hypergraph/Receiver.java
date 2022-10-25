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

package org.apache.doris.nereids.rules.exploration.join.hypergraph;

import com.google.common.base.Preconditions;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import java.util.BitSet;
import java.util.HashMap;

public class Receiver {
    // limit define the max number of csg-cmp pair in this Receiver
    int limit;
    HashMap<BitSet, Plan> planMap;
    Receiver(int limit) {
        this.limit = limit;
    }

    public boolean emitCsgCmp(BitSet left, BitSet right, Edge edge) {
        limit -= 1;
        if (limit < 0) {
            return false;
        }
        Preconditions.checkArgument(planMap.containsKey(left));
        Preconditions.checkArgument(planMap.containsKey(right));
        Plan plan = new LogicalJoin<>(edge.join.getJoinType(), planMap.get(left), planMap.get(right));
        left.or(right);
        planMap.put(left, plan);
        return true;
    }

    public void addPlan(Node node) {
        BitSet bitSet = new BitSet();
        bitSet.set(node.index);
        planMap.put(bitSet, node.getPlan());
    }

    public Plan getBestPlan(BitSet bitSet) {
        return planMap.get(bitSet);
    }
}
