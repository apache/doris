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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropDictionaryPersistInfo implements Writable {
    @SerializedName("db")
    private String dbName;

    @SerializedName("dic")
    private String dictionaryName;

    public DropDictionaryPersistInfo(String dbName, String dictionaryName) {
        this.dbName = dbName;
        this.dictionaryName = dictionaryName;
    }

    public static DropDictionaryPersistInfo read(DataInput in) throws IOException {
        String dbName = Text.readString(in);
        String dictionaryName = Text.readString(in);
        return new DropDictionaryPersistInfo(dbName, dictionaryName);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, dbName);
        Text.writeString(out, dictionaryName);
    }

    public String getDbName() {
        return dbName;
    }

    public String getDictionaryName() {
        return dictionaryName;
    }
}
