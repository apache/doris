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

package org.apache.doris.catalog;

import org.apache.doris.analysis.EncryptKeyName;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// used to search a EncryptKey
public class EncryptKeySearchDesc implements Writable {
    private EncryptKeyName encryptKeyName;

    private EncryptKeySearchDesc() {}

    public EncryptKeySearchDesc(EncryptKeyName encryptKeyName) {
        this.encryptKeyName = encryptKeyName;
    }

    public EncryptKeyName getKeyEncryptKeyName() {
        return encryptKeyName;
    }

    public boolean isIdentical(EncryptKey encryptKey) {
        if (encryptKeyName.equals(encryptKey.getEncryptKeyName())) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(encryptKeyName.toString());
        return stringBuilder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        encryptKeyName.write(out);
    }

    private void readFields(DataInput input) throws IOException {
        encryptKeyName = EncryptKeyName.read(input);
    }

    public static EncryptKeySearchDesc read(DataInput input) throws IOException {
        EncryptKeySearchDesc desc = new EncryptKeySearchDesc();
        desc.readFields(input);
        return desc;
    }
}
