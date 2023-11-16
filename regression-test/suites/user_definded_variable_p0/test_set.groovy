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

suite("test_set", "p0") {

    def sqlScript = """
            set @A=3;
            set @a=4;
        """

        // 执行SQL脚本
        sql sqlScript

        // 检查变量赋值结果
        def result = sql "select @a=@A as comparison_result"

        // 断言变量比较结果符合预期
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 1)
        assertTrue(result[0][0] == 1, "Variable comparison should be equal")
}
