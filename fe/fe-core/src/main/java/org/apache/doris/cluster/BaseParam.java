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

package org.apache.doris.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import com.google.common.collect.Lists;

public class BaseParam implements Writable {

    private final List<String> strParams = Lists.newArrayList();
    private final List<Long> longParams = Lists.newArrayList();
    private final List<Float> floatParams = Lists.newArrayList();
    // private final List<Integer> intParams = Lists.newArrayList();
    // private final List<Double> doubleParams = Lists.newArrayList();
    // private final List<Float> floatParams = Lists.newArrayList();

    public String getStringParam() {
        return strParams.get(0);
    }

    public String getStringParam(int index) {
        return strParams.get(index);
    }

    public void addStringParam(String value) {
        this.strParams.add(value);
    }

    public long getLongParam() {
        return longParams.get(0);
    }

    public long getLongParam(int index) {
        return longParams.get(index);
    }

    public void addLongParam(long value) {
        this.longParams.add(value);
    }

    public int getParamLength() {
        return strParams.size() + longParams.size();
    }

    public float getFloatParam(int index) {
        return floatParams.get(index);
    }

    public void addFloatParam(float value) {
        this.floatParams.add(value);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(strParams.size());
        for (String str : strParams) {
            Text.writeString(out, str);
        }

        out.writeInt(longParams.size());
        for (long value : longParams) {
            out.writeLong(value);
        }

        out.writeInt(floatParams.size());
        for (float value : floatParams) {
            out.writeFloat(value);
        }

    }

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        while (count-- > 0) {
            strParams.add(Text.readString(in));
        }

        count = in.readInt();
        while (count-- > 0) {
            longParams.add(in.readLong());
        }

        count = in.readInt();
        while (count-- > 0) {
            floatParams.add(in.readFloat());
        }

    }

}
