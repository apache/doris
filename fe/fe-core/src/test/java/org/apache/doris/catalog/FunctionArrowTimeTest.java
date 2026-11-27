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

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class FunctionArrowTimeTest {
    @Test
    void testPythonArrowTimeProtocolFlag() {
        Type datetime = ScalarType.createDatetimeV2Type(6);
        Type[] args = {datetime};
        ScalarFunction scalar = ScalarFunction.createUdf(TFunctionBinaryType.PYTHON_UDF,
                new FunctionName("db1", "python_scalar"), args, datetime,
                false, null, "evaluate", null, null);
        ScalarFunction table = ScalarFunction.createUdf(TFunctionBinaryType.PYTHON_UDF,
                new FunctionName("db1", "python_table"), args, datetime,
                false, null, "evaluate", null, null);
        table.setUDTFunction(true);
        AggregateFunction aggregate = new AggregateFunction(new FunctionName("db1", "python_aggregate"),
                Arrays.asList(args), datetime, datetime, null,
                "accumulate", "init", null, "merge", null, null, "finish");
        aggregate.setBinaryType(TFunctionBinaryType.PYTHON_UDF);
        for (Function fn : new Function[] {scalar, table, aggregate}) {
            fn.setRuntimeVersion("3.12.11");
            TFunction result = fn.toThrift(datetime, args, new Boolean[] {true});
            Assertions.assertTrue(result.isSetEnablePythonUdfArrowTimeV2());
            Assertions.assertTrue(result.isEnablePythonUdfArrowTimeV2());
        }
        scalar.setBinaryType(TFunctionBinaryType.JAVA_UDF);
        TFunction javaFunction = scalar.toThrift(datetime, args, new Boolean[] {true});
        Assertions.assertTrue(javaFunction.isSetEnablePythonUdfArrowTimeV2());
        Assertions.assertTrue(javaFunction.isEnablePythonUdfArrowTimeV2());
    }
}
