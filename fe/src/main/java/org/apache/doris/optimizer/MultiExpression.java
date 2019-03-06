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

package org.apache.doris.optimizer;

import org.apache.doris.optimizer.operator.OptOperator;

import java.util.List;
import java.util.Objects;

// MultiExpression is another way to represent OptExpression, which
// contains an operator and inputs.
// Because MultiExpression's inputs are Groups, so one MultiExpression
// equal with several logical equivalent Expression. As a result, this
// can reduce search space dramatically.
public class MultiExpression {
    private int id;
    private OptOperator op;
    private List<OptGroup>  inputs;

    // OptGroup which this MultiExpression belongs to. Firstly it's null when object is created,
    // it will be assigned after it is inserted into OptMemo
    private OptGroup group;

    public MultiExpression(OptOperator op, List<OptGroup> inputs) {
        this.op = op;
        this.inputs = inputs;
    }

    public void setId(int id) { this.id = id; }
    public int getId() { return id; }
    public OptOperator getOp() { return op; }
    public int arity() { return inputs.size(); }
    public List<OptGroup> getInputs() { return inputs; }
    public OptGroup getInput(int idx) { return inputs.get(idx); }

    public OptGroup getGroup() { return group; }

    // get next MultiExpression in same group
    public MultiExpression next() { return null; }

    public String debugString() {
        return "MultiExpression";
    }

    @Override
    public int hashCode() {
        int hash = op.hashCode();
        for (OptGroup group : inputs) {
            hash = OptUtils.combineHash(hash, group.hashCode());
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }
}
