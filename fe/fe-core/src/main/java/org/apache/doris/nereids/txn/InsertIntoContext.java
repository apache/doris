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

package org.apache.doris.nereids.txn;

import org.apache.doris.nereids.types.DataType;

import java.util.List;

/**
 * context for insert into command
 */
public class InsertIntoContext {
    private List<DataType> targetSchema = null;
    private int keyNums = 0;

    public void setTargetSchema(List<DataType> targetSchema) {
        this.targetSchema = targetSchema;
    }

    public List<DataType> getTargetSchema() {
        return targetSchema;
    }

    public void setKeyNums(int keyNums) {
        this.keyNums = keyNums;
    }

    public int getKeyNums() {
        return keyNums;
    }
}
