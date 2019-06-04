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

package org.apache.doris.optimizer.rule.implementation;

import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.OptLogicalScan;
import org.apache.doris.optimizer.operator.OptPhysicalOlapScan;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.rule.RuleCallContext;

import java.util.List;

public class OlapScanRule extends ImplemetationRule {

    public static OlapScanRule INSTANCE = new OlapScanRule();

    private OlapScanRule() {
        super(OptRuleType.RULE_IMP_OLAP_LSCAN_TO_PSCAN,
                OptExpression.create(
                        new OptLogicalScan()));
    }

    @Override
    public void transform(RuleCallContext call) {
        final OptLogicalScan scan = (OptLogicalScan)call.getOrigin().getOp();
        final OptExpression newExpression =
                OptExpression.create(new OptPhysicalOlapScan(scan.getTable(), scan.getOutputColumns()));
        call.addNewExpr(newExpression);
    }
}
