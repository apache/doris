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

import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TTypeDesc;

/**
 * Describes a AnyType type, used for SQL function return type,
 *  NOT used for table column type.
 */
public class AnyType extends Type {

    @Override
    protected String toSql(int depth) {
        return null;
    }

    @Override
    protected String prettyPrint(int lpad) {
        return null;
    }

    @Override
    public void toThrift(TTypeDesc container) {
        throw new RuntimeException("can not call toThrift on AnyType.");
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        throw new RuntimeException("can not call toColumnTypeThrift on AnyType");
    }
}
