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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.ExpressionRuleExecutor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

class NestedCaseWhenCondToLiteralTest extends ExpressionRewriteTestHelper {

    @Test
    void testNestedCaseWhen() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NestedCaseWhenCondToLiteral.INSTANCE)
        ));

        assertRewriteAfterTypeCoercion(
                "case when a > 1 then 1"
                        + "    when a > 2 then"
                        + "               (case when a > 1 then 2"
                        + "                     when a > 2 then 3"
                        + "                     when a > 3 then (case when a > 1 then 4"
                        + "                                           when a > 2 then 5"
                        + "                                           when a > 3 then 6"
                        + "                                      end)"
                        + "               end)"
                        + "    when (case when a > 1 then a > 1"
                        + "               when a > 2 then a > 2"
                        + "               when a > 3 then a > 3"
                        + "               when a > 1 then a > 1"
                        + "          end) then 100"
                        + "    when a > 3 then 7"
                        + "    when a > 1 then 8"
                        + "    else (case when a > 1 then 9"
                        + "               when a > 2 then 10"
                        + "               when a > 3 then 11"
                        + "               when a > 4 then 12"
                        + "               else (case when a > 1 then 13"
                        + "                          when a > 2 then 14"
                        + "                          when a > 3 then 15"
                        + "                          when a > 4 then 16"
                        + "                          when a > 5 then (case when a > 1 then 17 when a > 5 then 18 end)"
                        + "                     end)"
                        + "          end)"
                        + " end",
                "case when a > 1 then 1"
                        + "    when a > 2 then"
                        + "               (case when false then 2"
                        + "                     when true then 3"
                        + "                     when a > 3 then (case when false then 4"
                        + "                                           when true then 5"
                        + "                                       end)"
                        + "                end)"
                        + "    when (case when false then a > 1"
                        + "               when a > 3 then a > 3"
                        + "          end) then 100"
                        + "    when a > 3 then 7"
                        + "    else (case when false then 9"
                        + "               when a > 4 then 12"
                        + "               else (case when false then 13"
                        + "                          when a > 5 then (case when false then 17 when true then 18 end)"
                        + "                     end)"
                        + "          end)"
                        + " end"
        );
    }

    @Test
    void testNestedIf() {
        executor = new ExpressionRuleExecutor(ImmutableList.of(
                bottomUp(NestedCaseWhenCondToLiteral.INSTANCE)
        ));
        assertRewriteAfterTypeCoercion(
                "if("
                        + "      a > 1,"
                        + "      if("
                        + "              a > 1,"
                        + "              if("
                        + "                      a > 2,"
                        + "                      if(a > 2,a + 2,a + 3),"
                        + "                      if("
                        + "                                a > 1,"
                        + "                                if(a > 2,a + 3,a + 4),"
                        + "                                if(a > 2,a + 5,a + 6)"
                        + "                      )"
                        + "               ),"
                        + "               if(a > 1,a + 1,a + 2)"
                        + "       ),"
                        + "       if("
                        + "               a > 1,"
                        + "               a + 5,"
                        + "               if(a > 2,a + 6,a + 7)"
                        + "       )"
                        + ")",
                "if("
                        + "      a > 1,"
                        + "      if("
                        + "              true,"
                        + "              if("
                        + "                      a > 2,"
                        + "                      if(true,a + 2,a + 3),"
                        + "                      if("
                        + "                                true,"
                        + "                                if(false,a + 3,a + 4),"
                        + "                                if(false,a + 5,a + 6)"
                        + "                      )"
                        + "               ),"
                        + "               if(true,a + 1,a + 2)"
                        + "       ),"
                        + "       if("
                        + "               false,"
                        + "               a + 5,"
                        + "               if(a > 2,a + 6,a + 7)"
                        + "       )"
                        + ")"
        );
    }
}
