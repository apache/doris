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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/FunctionArgs.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;

// Wrapper class around argument types and if it has varArgs
public class FunctionArgs {
    public final ArrayList<Type> argTypes;
    public       boolean                  hasVarArgs;

    public FunctionArgs() {
        argTypes = Lists.newArrayList();
        hasVarArgs = false;
    }

    public FunctionArgs(ArrayList<Type> argTypes, boolean varArgs) {
        this.argTypes = argTypes;
        this.hasVarArgs = varArgs;
        if (varArgs) {
            Preconditions.checkState(argTypes.size() > 0);
        }
    }

    public void setHasVarArgs(boolean b) {
        hasVarArgs = b;
    }
}
