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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for user variable handling in expression analysis. */
public class UserVariableAnalysisTest {

    @Test
    public void testUserVarIntegerType() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        // set user var @a = TINY_INT_MAX (tiny int)
        ctx.setUserVar("a", new IntLiteral(Byte.MAX_VALUE));
        // set user var @a = SMALL_INT_MAX (small int)
        ctx.setUserVar("b", new IntLiteral(Short.MAX_VALUE));
        // set user var @b = Long.MAX_VALUE (bigint)
        ctx.setUserVar("c", new IntLiteral(Integer.MAX_VALUE));
        // set user var @b = Long.MAX_VALUE (bigint)
        ctx.setUserVar("d", new IntLiteral(Long.MAX_VALUE));
        // set user var @b = Long.MAX_VALUE (bigint)
        ctx.setUserVar("e", new LargeIntLiteral(LargeIntLiteral.LARGE_INT_MAX));

        Assertions.assertEquals(TinyIntType.INSTANCE, ConnectContext.get().getLiteralForUserVar("a").getDataType());
        Assertions.assertEquals(SmallIntType.INSTANCE, ConnectContext.get().getLiteralForUserVar("b").getDataType());
        Assertions.assertEquals(IntegerType.INSTANCE, ConnectContext.get().getLiteralForUserVar("c").getDataType());
        Assertions.assertEquals(BigIntType.INSTANCE, ConnectContext.get().getLiteralForUserVar("d").getDataType());
        Assertions.assertEquals(LargeIntType.INSTANCE, ConnectContext.get().getLiteralForUserVar("e").getDataType());
    }
}
