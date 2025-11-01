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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TupleDescriptor.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TColumnAccessPath;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/** AccessPathInfo */
@Data
@AllArgsConstructor
public class AccessPathInfo {
    private DataType prunedType;
    private List<TColumnAccessPath> allAccessPaths;
    private List<TColumnAccessPath> predicateAccessPaths;
}
