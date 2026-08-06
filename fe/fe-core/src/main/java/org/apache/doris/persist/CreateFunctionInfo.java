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

package org.apache.doris.persist;

import org.apache.doris.catalog.Function;
import org.apache.doris.common.io.Writable;

import com.google.common.collect.ImmutableList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CreateFunctionInfo implements Writable {
    private final List<Function> functions;

    public CreateFunctionInfo(List<Function> functions) {
        this.functions = ImmutableList.copyOf(functions);
    }

    public List<Function> getFunctions() {
        return functions;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(functions.size());
        for (Function function : functions) {
            function.write(out);
        }
    }

    public static CreateFunctionInfo read(DataInput in) throws IOException {
        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        int functionSize = in.readInt();
        for (int i = 0; i < functionSize; i++) {
            builder.add(Function.read(in));
        }
        return new CreateFunctionInfo(builder.build());
    }
}
