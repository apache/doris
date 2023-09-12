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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;

import org.junit.jupiter.api.Test;

class LiteralTest {
    // MySQL Date的组成
    // 1. 没有任何分隔符 eg: 20220801, 20220801010101
    //    MySQL 支持 20220801T010101 但是不支持 20220801 010101
    //    MySQL 这种情况下不支持 zone / offset
    // 2. 有分隔符
    //    分隔符可以为 ' '/ 'T'
    //    组成为 Date + delimiter + Time + Zone + Offset
    //    其中 Zone 和 Offset 是 Optional
    //    Date 需要注意 two-digit year https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    //      Dates containing 2-digit year values are ambiguous because the century is unknown. MySQL interprets 2-digit year values using these rules:
    //          Year values in the range 00-69 become 2000-2069.
    //          Year values in the range 70-99 become 1970-1999.
    //    Time 需要注意 microsecond
    //      需要注意 不完全time 'hh:mm:ss', 'hh:mm', 'D hh:mm', 'D hh', or 'ss'
}
