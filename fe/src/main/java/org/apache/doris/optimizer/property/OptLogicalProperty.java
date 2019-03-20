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

package org.apache.doris.optimizer.property;

import com.google.common.collect.Lists;
import org.apache.doris.optimizer.OptExpressionWapper;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.operator.OptLogical;

import java.util.List;

public class OptLogicalProperty extends OptProperty {

    private List<OptColumnRef> outputs;

    public OptLogicalProperty() {
        this.outputs = Lists.newArrayList();
    }

    public List<OptColumnRef> getOutputs() { return outputs; }
    public void setOutputs(List<OptColumnRef> columns) { outputs.addAll(columns); }

    @Override
    public void derive(OptExpressionWapper wapper, List<OptProperty> childrenProperty) {
        // Derive columns
        final OptLogical logical = (OptLogical) wapper.getExpression().getOp();
        outputs.addAll(logical.deriveOuput(wapper));
    }
}
