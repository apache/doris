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

package org.apache.doris.nereids.hint;

import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Outline manager used to manage read write and cached of outline
 */
public class OutlineMgr implements Writable {
    public static final OutlineMgr INSTANCE = new OutlineMgr();
    private static final Map<String, OutlineInfo> outlineMap = new HashMap<>();

    public static Optional<OutlineInfo> getOutline(String outlineName) {
        return Optional.of(outlineMap.get(outlineName));
    }

    public static void addOutline(String outlineName, OutlineInfo outlineInfo) {
        outlineMap.put(outlineName, outlineInfo);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(outlineMap.size());
        for (OutlineInfo outlineInfo : outlineMap.values()) {
            outlineInfo.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            OutlineInfo outlineInfo = OutlineInfo.read(in);
            outlineMap.put(outlineInfo.getOutlineName(), outlineInfo);
        }
    }
}
