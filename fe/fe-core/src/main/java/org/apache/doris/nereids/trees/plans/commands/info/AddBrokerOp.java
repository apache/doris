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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.ModifyBrokerClause;

import java.util.List;

/**
 * AddBrokerOp
 */
public class AddBrokerOp extends BrokerOp {
    public AddBrokerOp(String brokerName, List<String> hostPorts) {
        super(ModifyOp.OP_ADD, brokerName, hostPorts);
    }

    @Override
    public AlterClause translateToLegacyAlterClause() {
        return new ModifyBrokerClause(ModifyBrokerClause.ModifyOp.OP_ADD,
                getBrokerName(), getHostPorts(), getHostPortPairs());
    }
}
