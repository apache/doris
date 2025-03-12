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

package org.apache.doris.common.jni.utils;

import org.apache.doris.catalog.StructField;
import org.apache.doris.thrift.TPrimitiveType;

import java.util.ArrayList;

public class JavaUdfStructType extends JavaUdfDataType {
    private ArrayList<StructField> fields;

    public JavaUdfStructType(String description, TPrimitiveType thriftType, int len) {
        super(description, thriftType, len);
        this.fields = new ArrayList<>();
    }

    public JavaUdfStructType(JavaUdfStructType other) {
        super(other);
        this.fields = other.fields;
    }

    public JavaUdfStructType(ArrayList<StructField> fields) {
        super("STRUCT_TYPE", TPrimitiveType.STRUCT, 0);
        this.fields = fields;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder(super.toString());
        for (StructField field : fields) {
            res.append(" field: ").append(field.getName()).append(" type: ").append(field.getType().toString());
        }
        return res.toString();
    }

    public void setFields(ArrayList<StructField> fields) {
        this.fields = fields;
    }

    public ArrayList<StructField> getFields() {
        return fields;
    }

    public ArrayList<String> getFieldNames() {
        ArrayList<String> names = new ArrayList<>();
        for (StructField filed : fields) {
            names.add(filed.getName());
        }
        return names;
    }
}
