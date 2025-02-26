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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * BrokerOp
 */
public abstract class BrokerOp extends AlterSystemOp {
    protected Set<Pair<String, Integer>> hostPortPairs;
    private final ModifyOp op;
    private final String brokerName;
    private final List<String> hostPorts;

    /**
     * ModifyOp
     */
    public enum ModifyOp {
        OP_ADD,
        OP_DROP,
        OP_DROP_ALL
    }

    public BrokerOp(ModifyOp op, String brokerName, List<String> hostPorts) {
        super(AlterOpType.ALTER_OTHER);
        this.op = op;
        this.brokerName = brokerName;
        this.hostPorts = hostPorts;
    }

    public ModifyOp getOp() {
        return op;
    }

    public List<String> getHostPorts() {
        return hostPorts;
    }

    /**
     * getBrokerName
     */
    public String getBrokerName() {
        return brokerName;
    }

    /**
     * getHostPortPairs
     */
    public Set<Pair<String, Integer>> getHostPortPairs() {
        return hostPortPairs;
    }

    /**
     * validateBrokerName
     */
    private void validateBrokerName() throws AnalysisException {
        if (Strings.isNullOrEmpty(brokerName)) {
            throw new AnalysisException("Broker's name can't be empty.");
        }
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        validateBrokerName();

        if (op != ModifyOp.OP_DROP_ALL) {
            hostPortPairs = Sets.newHashSet();
            for (String hostPort : hostPorts) {
                Pair<String, Integer> pair = SystemInfoService.validateHostAndPort(hostPort);
                hostPortPairs.add(pair);
            }
            Preconditions.checkState(!hostPortPairs.isEmpty());
        }
    }

    @Override
    public String toSql() {
        throw new NotImplementedException("toSql not implemented");
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException("getProperties not implemented");
    }
}
