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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * SELECT * FROM lineorder ORDER BY 'f' -> SELECT * FROM lineorder
 */
public class EliminateOrderByConstant extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalSort().then(sort -> {
            List<OrderKey> orderKeys = sort.getOrderKeys();
            ImmutableList.Builder<OrderKey> orderKeysWithoutConstBuilder
                    = ImmutableList.builderWithExpectedSize(orderKeys.size());
            for (OrderKey orderKey : orderKeys) {
                if (!orderKey.getExpr().isConstant()) {
                    orderKeysWithoutConstBuilder.add(orderKey);
                }
            }
            List<OrderKey> orderKeysWithoutConst = orderKeysWithoutConstBuilder.build();
            if (orderKeysWithoutConst.isEmpty()) {
                return sort.child();
            } else if (orderKeysWithoutConst.size() == orderKeys.size()) {
                return sort;
            }
            return sort.withOrderKeys(orderKeysWithoutConst);
        }).toRule(RuleType.ELIMINATE_ORDER_BY_CONSTANT);
    }

}
