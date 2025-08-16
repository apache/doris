## Guidebook - Good first issue: How to implement a new SQL function

What to do

To implement a new SQL function, here's what you need to write in your PR:

The function implementation and registration in BE
The function signature and visitor for nereids planner in FE
The constant fold implementation in FE if possible. just like what https://github.com/apache/doris/pull/40744/files did in functions/executable/NumericArithmetic.java.
Enough regression-test and BE-UT cases, referring files test_template_{X}_arg(s).groovy in https://github.com/apache/doris/pull/47307/files (maybe updated. So find the newest version in master branch)
You could refer to https://github.com/apache/doris/pull/47307/files as a complete example(only missing FE constant folding)

btw: You may see some PR modified doris_builtin_functions.py. Now we don't need it anymore.

Aggregation Functions

You can refer code structure for implementing an aggregation function at https://github.com/apache/doris/pull/41240/files. It includes what you need to add.

Key Points

BE Implementations

Use the base template when you try to implement a date/arithmetic calculation. You can find them by searching for other similar functions.
Execution speed is very, very important for Doris. Therefore, you must eliminate all unnecessary copies and calculations. Try to use raw operations on inputs and outputs. If you can use the output Column's memory to receive the calculation result, do not add another variable and copy them. Don't call any virtual function in a loop. If it's necessary, use the template to generate different function entities to eliminate type judgment.
You should not only pay attention to the new code you have added, but also consider its relevance to the existing code. If there is, please consider them together to achieve the best level of abstraction processing.
FE Signature

Most functions use one of the following interfaces:

AlwaysNullable means the function's return type is always wrapped in Nullable. Use it when the function may generate the null value for not-null input.
AlwaysNotNullable means the function's return type is never wrapped in Nullable. Use it when the function changes all the null input to a not-null output.
PropagateNullable: when the input columns contain at least one Nullable column, the output column is Nullable. otherwise not. When you calculate the result for a not-null value and leave null alone, it's the right choice.
Testcase

The testcases' type and quantities must not be less than the corresponding files in https://github.com/apache/doris/pull/47307/files.

The data you use must cover all the borders of its datatype and other sensitive values.

Add BE-UT case with check_function_all_arg_comb interface to cover Const-combinations.

You can run the cases using the scripts run-regression-test.sh and run-be-ut.sh. They have details explanations in them.

## How to correctly handle const columns in functions

New handling approach for ColumnConst in Function
Historically, we often used convert_to_full_column_if_const on parameter columns. In most cases, for a const column we only reuse its single value repeatedly; we don’t need to actually expand it. Expanding causes large overhead with many rows. We introduce two helpers to replace the old approach: unpack_if_const and default_preprocess_parameter_columns.
Enforcing const arguments
If some arguments must be const, override get_arguments_that_are_always_constant in BE to auto-validate. The corresponding argument will then be ensured as ColumnConst.
Single-argument case
No extra handling is needed; just keep use_default_implementation_for_constants = true (default is true if not overridden).
Q: When should it be overridden to false?
A: For non-deterministic functions like random, where same input may produce different outputs.
With get_arguments_that_are_always_constant set, a required-const argument will be ColumnConst; otherwise it will certainly be non-ColumnConst. Both cases have known types; no need to call convert_to_full_column_if_const.
For two or more arguments, also keep use_default_implementation_for_constants = true. Then we only need to implement vector_const without const_const (which is equivalent to vector_vector).
Two-argument case
For example, in FunctionBinaryToType, both inputs are data columns. Use:
const auto& [left_column, left_const] = unpack_if_const(block.get_by_position(arguments[0]).column);
const auto& [right_column, right_const] = unpack_if_const(block.get_by_position(arguments[1]).column);
This gives you the actual data `ColumnPtr` and whether it is `ColumnConst`. Then implement vector_vector and vector_scalar for different constancy patterns. See function_totype.h and the corresponding implementations in function_bitmap.cpp.
Multi-argument case
Some functions take many or variadic args, e.g., least & greatest. Then we can:
Columns cols(arguments.size());
std::unique_ptr<bool[]> col_const = std::make_unique<bool[]>(arguments.size());
for (int i = 0; i < arguments.size(); ++i) {
    std::tie(cols[i], col_const[i]) =
            unpack_if_const(block.get_by_position(arguments[i]).column);
}
When accessing each column’s data, use index_check_const to normalize the index if needed. See CompareMultiImpl for reference.
Non-data arguments
Many functions take 2+ columns, but only one is a “data” column; the rest are configuration-like parameters (e.g., substring, conv). Even if many columns make optimization harder, we can special-case ColumnConst parameter columns. For example, first column is data, columns 2 and 3 are parameters:
bool col_const[3];
ColumnPtr argument_columns[3];
for (int i = 0; i < 3; ++i) {
    col_const[i] = is_column_const(*block.get_by_position(arguments[i]).column);
}
argument_columns[0] = col_const[0] ? static_cast<const ColumnConst&>(
                                             *block.get_by_position(arguments[0]).column)
                                             .convert_to_full_column()
                                   : block.get_by_position(arguments[0]).column;

default_preprocess_parameter_columns(argument_columns, col_const, {1, 2}, block, arguments);
default_preprocess_parameter_columns will automatically decide whether parameter columns should be expanded. Then we can branch only on whether all parameter columns are const:
if (col_const[1] && col_const[2]) {
    execute_impl_scalar_args(...);
} else {
    execute_impl(...);
}
This reduces the problem to a two-column case. See FunctionBitmapSubs for an example.
Additionally, nullable handling can become verbose after these changes; you can use the helper check_set_nullable to extract/set nullable. In most cases you don’t need to call it explicitly; keeping use_default_implementation_for_constants = true lets the framework implement PropagateNullable automatically for the function.

## Reference PR mentioned by the guidebook

https://github.com/apache/doris/pull/47307/files

diff --git a/be/src/vec/functions/function_compress.cpp b/be/src/vec/functions/function_compress.cpp
new file mode 100644
index 00000000000000..4c175a5fd44379
--- /dev/null
+++ b/be/src/vec/functions/function_compress.cpp
@@ -0,0 +1,209 @@
+// Licensed to the Apache Software Foundation (ASF) under one
+// or more contributor license agreements.  See the NOTICE file
+// distributed with this work for additional information
+// regarding copyright ownership.  The ASF licenses this file
+// to you under the Apache License, Version 2.0 (the
+// "License"); you may not use this file except in compliance
+// with the License.  You may obtain a copy of the License at
+//
+//   http://www.apache.org/licenses/LICENSE-2.0
+//
+// Unless required by applicable law or agreed to in writing,
+// software distributed under the License is distributed on an
+// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
+// KIND, either express or implied.  See the License for the
+// specific language governing permissions and limitations
+// under the License.
+#include <glog/logging.h>
+
+#include <array>
+#include <cctype>
+#include <cstddef>
+#include <cstring>
+#include <functional>
+#include <memory>
+#include <string>
+#include <utility>
+
+#include "common/status.h"
+#include "util/block_compression.h"
+#include "util/faststring.h"
+#include "vec/aggregate_functions/aggregate_function.h"
+#include "vec/columns/column.h"
+#include "vec/columns/column_nullable.h"
+#include "vec/columns/column_string.h"
+#include "vec/columns/column_vector.h"
+#include "vec/columns/columns_number.h"
+#include "vec/common/assert_cast.h"
+#include "vec/core/block.h"
+#include "vec/core/column_numbers.h"
+#include "vec/core/column_with_type_and_name.h"
+#include "vec/core/types.h"
+#include "vec/data_types/data_type.h"
+#include "vec/data_types/data_type_nullable.h"
+#include "vec/data_types/data_type_number.h"
+#include "vec/data_types/data_type_string.h"
+#include "vec/functions/function.h"
+#include "vec/functions/simple_function_factory.h"
+
+namespace doris {
+class FunctionContext;
+} // namespace doris
+
+namespace doris::vectorized {
+
+class FunctionCompress : public IFunction {
+public:
+    static constexpr auto name = "compress";
+    static FunctionPtr create() { return std::make_shared<FunctionCompress>(); }
+
+    String get_name() const override { return name; }
+
+    size_t get_number_of_arguments() const override { return 1; }
+
+    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
+        return std::make_shared<DataTypeString>();
+    }
+
+    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
+                        uint32_t result, size_t input_rows_count) const override {
+        // Get the compression algorithm object
+        BlockCompressionCodec* compression_codec;
+        RETURN_IF_ERROR(get_block_compression_codec(segment_v2::CompressionTypePB::ZLIB,
+                                                    &compression_codec));
+
+        const auto& arg_column =
+                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);
+        auto result_column = ColumnString::create();
+
+        auto& arg_data = arg_column.get_chars();
+        auto& arg_offset = arg_column.get_offsets();
+        const char* arg_begin = reinterpret_cast<const char*>(arg_data.data());
+
+        auto& col_data = result_column->get_chars();
+        auto& col_offset = result_column->get_offsets();
+        col_offset.resize(input_rows_count);
+
+        faststring compressed_str;
+        Slice data;
+
+        // When the original string is large, the result is roughly this value
+        size_t total = arg_offset[input_rows_count - 1];
+        col_data.reserve(total / 1000);
+
+        for (size_t row = 0; row < input_rows_count; row++) {
+            uint32_t length = arg_offset[row] - arg_offset[row - 1];
+            data = Slice(arg_begin + arg_offset[row - 1], length);
+
+            size_t idx = col_data.size();
+            if (!length) { // data is ''
+                col_offset[row] = col_offset[row - 1];
+                continue;
+            }
+
+            // Z_MEM_ERROR and Z_BUF_ERROR are already handled in compress, making sure st is always Z_OK
+            auto st = compression_codec->compress(data, &compressed_str);
+            col_data.resize(col_data.size() + 4 + compressed_str.size());
+
+            std::memcpy(col_data.data() + idx, &length, sizeof(length));
+            idx += 4;
+
+            // The length of compress_str is not known in advance, so it cannot be compressed directly into col_data
+            unsigned char* src = compressed_str.data();
+            for (size_t i = 0; i < compressed_str.size(); idx++, i++, src++) {
+                col_data[idx] = *src;
+            }
+            col_offset[row] = col_offset[row - 1] + 10 + compressed_str.size();
+        }
+
+        block.replace_by_position(result, std::move(result_column));
+        return Status::OK();
+    }
+};
+
+class FunctionUncompress : public IFunction {
+public:
+    static constexpr auto name = "uncompress";
+    static FunctionPtr create() { return std::make_shared<FunctionUncompress>(); }
+
+    String get_name() const override { return name; }
+
+    size_t get_number_of_arguments() const override { return 1; }
+
+    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
+        return make_nullable(std::make_shared<DataTypeString>());
+    }
+
+    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
+                        uint32_t result, size_t input_rows_count) const override {
+        // Get the compression algorithm object
+        BlockCompressionCodec* compression_codec;
+        RETURN_IF_ERROR(get_block_compression_codec(segment_v2::CompressionTypePB::ZLIB,
+                                                    &compression_codec));
+
+        const auto& arg_column =
+                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);
+
+        auto& arg_data = arg_column.get_chars();
+        auto& arg_offset = arg_column.get_offsets();
+        const char* arg_begin = reinterpret_cast<const char*>(arg_data.data());
+
+        auto result_column = ColumnString::create();
+        auto& col_data = result_column->get_chars();
+        auto& col_offset = result_column->get_offsets();
+        col_offset.resize(input_rows_count);
+
+        auto null_column = ColumnUInt8::create(input_rows_count);
+        auto& null_map = null_column->get_data();
+
+        std::string uncompressed;
+        Slice data;
+        Slice uncompressed_slice;
+
+        size_t total = arg_offset[input_rows_count - 1];
+        col_data.reserve(total * 1000);
+
+        for (size_t row = 0; row < input_rows_count; row++) {
+            null_map[row] = false;
+            data = Slice(arg_begin + arg_offset[row - 1], arg_offset[row] - arg_offset[row - 1]);
+            size_t data_length = arg_offset[row] - arg_offset[row - 1];
+
+            if (data_length == 0) { // The original data is ''
+                col_offset[row] = col_offset[row - 1];
+                continue;
+            }
+
+            union {
+                char bytes[4];
+                uint32_t value;
+            } length;
+            std::memcpy(length.bytes, data.data, 4);
+
+            size_t idx = col_data.size();
+            col_data.resize(col_data.size() + length.value);
+            uncompressed_slice = Slice(col_data.data() + idx, length.value);
+
+            Slice compressed_data(data.data + 4, data.size - 4);
+            auto st = compression_codec->decompress(compressed_data, &uncompressed_slice);
+
+            if (!st.ok()) {                                      // is not a legal compressed string
+                col_data.resize(col_data.size() - length.value); // remove compressed_data
+                col_offset[row] = col_offset[row - 1];
+                null_map[row] = true;
+                continue;
+            }
+            col_offset[row] = col_offset[row - 1] + length.value;
+        }
+
+        block.replace_by_position(
+                result, ColumnNullable::create(std::move(result_column), std::move(null_column)));
+        return Status::OK();
+    }
+};
+
+void register_function_compress(SimpleFunctionFactory& factory) {
+    factory.register_function<FunctionCompress>();
+    factory.register_function<FunctionUncompress>();
+}
+
+} // namespace doris::vectorized
diff --git a/be/src/vec/functions/simple_function_factory.h b/be/src/vec/functions/simple_function_factory.h
index 98f2917d163e31..46eca0cb419294 100644
--- a/be/src/vec/functions/simple_function_factory.h
+++ b/be/src/vec/functions/simple_function_factory.h
@@ -110,6 +110,7 @@ void register_function_ip(SimpleFunctionFactory& factory);
 void register_function_multi_match(SimpleFunctionFactory& factory);
 void register_function_split_by_regexp(SimpleFunctionFactory& factory);
 void register_function_assert_true(SimpleFunctionFactory& factory);
+void register_function_compress(SimpleFunctionFactory& factory);
 void register_function_bit_test(SimpleFunctionFactory& factory);
 
 class SimpleFunctionFactory {
@@ -301,6 +302,7 @@ class SimpleFunctionFactory {
             register_function_split_by_regexp(instance);
             register_function_assert_true(instance);
             register_function_bit_test(instance);
+            register_function_compress(instance);
         });
         return instance;
     }
diff --git a/fe/fe-core/src/main/java/org/apache/doris/catalog/BuiltinScalarFunctions.java b/fe/fe-core/src/main/java/org/apache/doris/catalog/BuiltinScalarFunctions.java
index b173383ff0c6ab..233128015cc089 100644
--- a/fe/fe-core/src/main/java/org/apache/doris/catalog/BuiltinScalarFunctions.java
+++ b/fe/fe-core/src/main/java/org/apache/doris/catalog/BuiltinScalarFunctions.java
@@ -119,6 +119,7 @@
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Char;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.CharacterLength;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
+import org.apache.doris.nereids.trees.expressions.functions.scalar.Compress;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.ConcatWs;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
@@ -449,6 +450,7 @@
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Trim;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.TrimIn;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Truncate;
+import org.apache.doris.nereids.trees.expressions.functions.scalar.Uncompress;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Unhex;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Upper;
@@ -974,7 +976,9 @@ public class BuiltinScalarFunctions implements FunctionHelper {
             scalar(YearsSub.class, "years_sub"),
             scalar(MultiMatch.class, "multi_match"),
             scalar(SessionUser.class, "session_user"),
-            scalar(LastQueryId.class, "last_query_id"));
+            scalar(LastQueryId.class, "last_query_id"),
+            scalar(Compress.class, "compress"),
+            scalar(Uncompress.class, "uncompress"));
 
     public static final BuiltinScalarFunctions INSTANCE = new BuiltinScalarFunctions();
 
diff --git a/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/scalar/Compress.java b/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/scalar/Compress.java
new file mode 100644
index 00000000000000..9422d72bca793a
--- /dev/null
+++ b/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/scalar/Compress.java
@@ -0,0 +1,69 @@
+// Licensed to the Apache Software Foundation (ASF) under one
+// or more contributor license agreements.  See the NOTICE file
+// distributed with this work for additional information
+// regarding copyright ownership.  The ASF licenses this file
+// to you under the Apache License, Version 2.0 (the
+// "License"); you may not use this file except in compliance
+// with the License.  You may obtain a copy of the License at
+//
+//   http://www.apache.org/licenses/LICENSE-2.0
+//
+// Unless required by applicable law or agreed to in writing,
+// software distributed under the License is distributed on an
+// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
+// KIND, either express or implied.  See the License for the
+// specific language governing permissions and limitations
+// under the License.
+
+package org.apache.doris.nereids.trees.expressions.functions.scalar;
+
+import org.apache.doris.catalog.FunctionSignature;
+import org.apache.doris.nereids.trees.expressions.Expression;
+import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
+import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
+import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
+import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
+import org.apache.doris.nereids.types.StringType;
+import org.apache.doris.nereids.types.VarcharType;
+
+import com.google.common.base.Preconditions;
+import com.google.common.collect.ImmutableList;
+
+import java.util.List;
+
+/**
+ * ScalarFunction 'compress'.
+ */
+public class Compress extends ScalarFunction
+        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullable {
+
+    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
+            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(VarcharType.SYSTEM_DEFAULT),
+            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE));
+
+    /**
+     * constructor with 1 argument.
+     */
+    public Compress(Expression arg) {
+        super("compress", arg);
+    }
+
+    /**
+     * withChildren.
+     */
+    @Override
+    public Compress withChildren(List<Expression> children) {
+        Preconditions.checkArgument(children.size() == 1);
+        return new Compress(children.get(0));
+    }
+
+    @Override
+    public List<FunctionSignature> getSignatures() {
+        return SIGNATURES;
+    }
+
+    @Override
+    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
+        return visitor.visitCompress(this, context);
+    }
+}
diff --git a/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/scalar/Uncompress.java b/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/scalar/Uncompress.java
new file mode 100644
index 00000000000000..8726963f486fce
--- /dev/null
+++ b/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/functions/scalar/Uncompress.java
@@ -0,0 +1,69 @@
+// Licensed to the Apache Software Foundation (ASF) under one
+// or more contributor license agreements.  See the NOTICE file
+// distributed with this work for additional information
+// regarding copyright ownership.  The ASF licenses this file
+// to you under the Apache License, Version 2.0 (the
+// "License"); you may not use this file except in compliance
+// with the License.  You may obtain a copy of the License at
+//
+//   http://www.apache.org/licenses/LICENSE-2.0
+//
+// Unless required by applicable law or agreed to in writing,
+// software distributed under the License is distributed on an
+// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
+// KIND, either express or implied.  See the License for the
+// specific language governing permissions and limitations
+// under the License.
+
+package org.apache.doris.nereids.trees.expressions.functions.scalar;
+
+import org.apache.doris.catalog.FunctionSignature;
+import org.apache.doris.nereids.trees.expressions.Expression;
+import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
+import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
+import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
+import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
+import org.apache.doris.nereids.types.StringType;
+import org.apache.doris.nereids.types.VarcharType;
+
+import com.google.common.base.Preconditions;
+import com.google.common.collect.ImmutableList;
+
+import java.util.List;
+
+/**
+ * ScalarFunction 'uncompress'.
+ */
+public class Uncompress extends ScalarFunction
+        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNullable {
+
+    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
+            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(VarcharType.SYSTEM_DEFAULT),
+            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE));
+
+    /**
+     * constructor with 1 argument.
+     */
+    public Uncompress(Expression arg) {
+        super("uncompress", arg);
+    }
+
+    /**
+     * withChildren.
+     */
+    @Override
+    public Uncompress withChildren(List<Expression> children) {
+        Preconditions.checkArgument(children.size() == 1);
+        return new Uncompress(children.get(0));
+    }
+
+    @Override
+    public List<FunctionSignature> getSignatures() {
+        return SIGNATURES;
+    }
+
+    @Override
+    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
+        return visitor.visitUncompress(this, context);
+    }
+}
diff --git a/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/visitor/ScalarFunctionVisitor.java b/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/visitor/ScalarFunctionVisitor.java
index 1a41ba4f23eb97..4d24a57d64c0f9 100644
--- a/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/visitor/ScalarFunctionVisitor.java
+++ b/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/expressions/visitor/ScalarFunctionVisitor.java
@@ -126,6 +126,7 @@
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Char;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.CharacterLength;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
+import org.apache.doris.nereids.trees.expressions.functions.scalar.Compress;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.ConcatWs;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
@@ -446,6 +447,7 @@
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Trim;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.TrimIn;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Truncate;
+import org.apache.doris.nereids.trees.expressions.functions.scalar.Uncompress;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Unhex;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.UnixTimestamp;
 import org.apache.doris.nereids.trees.expressions.functions.scalar.Upper;
@@ -2328,4 +2330,12 @@ default R visitMultiMatch(MultiMatch multiMatch, C context) {
     default R visitLastQueryId(LastQueryId queryId, C context) {
         return visitScalarFunction(queryId, context);
     }
+
+    default R visitCompress(Compress compress, C context) {
+        return visitScalarFunction(compress, context);
+    }
+
+    default R visitUncompress(Uncompress uncompress, C context) {
+        return visitScalarFunction(uncompress, context);
+    }
 }
diff --git a/regression-test/data/query_p0/sql_functions/string_functions/test_compress_uncompress.out b/regression-test/data/query_p0/sql_functions/string_functions/test_compress_uncompress.out
new file mode 100644
index 00000000000000..be60951c955392
--- /dev/null
+++ b/regression-test/data/query_p0/sql_functions/string_functions/test_compress_uncompress.out
@@ -0,0 +1,38 @@
+-- This file is automatically generated. You should know what you did if you want to edit this
+-- !restore_original_data --
+1	Hello, world!
+2	Doris测试中文字符
+4	
+5	\N
+6	aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
+
+-- !uncompress_null_input --
+3	\N
+
+-- !uncompress_invalid_data --
+5	\N
+
+-- !compress_empty_string --
+4	
+
+-- !compress_repeated_string --
+6	aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
+
+-- !compress_single_char --
+x
+
+-- !compress_null_text --
+\N
+
+-- !compress_large_repeated_string --
+aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
+
+-- !compress_empty_string_direct --
+
+
+-- !compress_string_direct --
+Hello, world!
+
+-- !compress_numeric_direct --
+12345
+
diff --git a/regression-test/suites/query_p0/sql_functions/string_functions/test_compress_uncompress.groovy b/regression-test/suites/query_p0/sql_functions/string_functions/test_compress_uncompress.groovy
new file mode 100644
index 00000000000000..9c4df7b1ec97be
--- /dev/null
+++ b/regression-test/suites/query_p0/sql_functions/string_functions/test_compress_uncompress.groovy
@@ -0,0 +1,139 @@
+// Licensed to the Apache Software Foundation (ASF) under one
+// or more contributor license agreements.  See the NOTICE file
+// distributed with this work for additional information
+// regarding copyright ownership.  The ASF licenses this file
+// to you under the Apache License, Version 2.0 (the
+// "License"); you may not use this file except in compliance
+// with the License.  You may obtain a copy of the License at
+//
+//   http://www.apache.org/licenses/LICENSE-2.0
+//
+// Unless required by applicable law or agreed to in writing,
+// software distributed under the License is distributed on an
+// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
+// KIND, either express or implied.  See the License for the
+// specific language governing permissions and limitations
+// under the License.
+
+suite("test_compress_uncompress") {
+    // Drop the existing table
+    sql "DROP TABLE IF EXISTS test_compression"
+
+    // Create the test table
+    sql """
+        CREATE TABLE test_compression (
+            k0 INT,                      -- Primary key
+            text_col STRING,             -- String column for input data
+            binary_col STRING            -- Binary column for compressed data
+        )
+        DISTRIBUTED BY HASH(k0)
+        PROPERTIES (
+            "replication_num" = "1"
+        );
+    """
+
+    // Insert test data with various cases
+    sql """
+        INSERT INTO test_compression VALUES
+        (1, 'Hello, world!', COMPRESS('Hello, world!')),        -- Plain string
+        (2, 'Doris测试中文字符', COMPRESS('Doris测试中文字符')),  -- Chinese characters
+        (3, NULL, NULL),                                        -- Null values
+        (4, '', COMPRESS('')),                                  -- Empty string
+        (5, NULL, 'invalid_compressed_data'),                   -- Invalid binary data
+        (6, REPEAT('a', 50), COMPRESS(REPEAT('a', 50)));        -- Short repeated string
+    """
+
+    // Test 1: Verify that UNCOMPRESS can correctly restore the original data
+    order_qt_restore_original_data """
+        SELECT 
+            k0,
+            UNCOMPRESS(binary_col) AS decompressed_data
+        FROM test_compression
+        WHERE binary_col IS NOT NULL
+        ORDER BY k0;
+    """
+
+    // Test 2: Verify that UNCOMPRESS returns NULL for NULL input
+    order_qt_uncompress_null_input """
+        SELECT 
+            k0, 
+            UNCOMPRESS(binary_col) AS decompressed_data
+        FROM test_compression
+        WHERE binary_col IS NULL
+        ORDER BY k0;
+    """
+
+    // Test 3: Verify that UNCOMPRESS handles invalid binary data gracefully
+    order_qt_uncompress_invalid_data """
+        SELECT 
+            k0, 
+            UNCOMPRESS(binary_col) AS decompressed_data
+        FROM test_compression
+        WHERE k0 = 5
+        ORDER BY k0;
+    """
+
+    // Test 4: Verify that COMPRESS and UNCOMPRESS work correctly with empty strings
+    order_qt_compress_empty_string """
+        SELECT 
+            k0,  
+            UNCOMPRESS(binary_col) AS decompressed_data
+        FROM test_compression
+        WHERE k0 = 4
+        ORDER BY k0;
+    """
+
+    // Test 5: Verify that COMPRESS and UNCOMPRESS work correctly with repeated strings
+    order_qt_compress_repeated_string """
+        SELECT 
+            k0,  
+            UNCOMPRESS(binary_col) AS decompressed_data
+        FROM test_compression
+        WHERE k0 = 6
+        ORDER BY k0;
+    """
+
+    // Additional tests using SELECT UNCOMPRESS(COMPRESS()) directly
+
+    // Test 6: Verify that COMPRESS and UNCOMPRESS work with a single character string
+    order_qt_compress_single_char """
+        SELECT 
+            UNCOMPRESS(COMPRESS('x')) AS decompressed_data
+        LIMIT 1;
+    """
+    
+    // Test 7: Verify that COMPRESS handles NULL text values correctly
+    order_qt_compress_null_text """
+        SELECT 
+            UNCOMPRESS(COMPRESS(NULL)) AS decompressed_data
+        LIMIT 1;
+    """
+
+    // Test 8: Verify that COMPRESS and UNCOMPRESS work with long repeated strings
+    order_qt_compress_large_repeated_string """
+        SELECT 
+            UNCOMPRESS(COMPRESS(REPEAT('a', 100))) AS decompressed_data
+        LIMIT 1;
+    """
+
+    // Test 9: Verify that COMPRESS and UNCOMPRESS work with an empty string
+    order_qt_compress_empty_string_direct """
+        SELECT 
+            UNCOMPRESS(COMPRESS('')) AS decompressed_data
+        LIMIT 1;
+    """
+
+    // Test 10: Verify that COMPRESS and UNCOMPRESS work with the string 'Hello, world!'
+    order_qt_compress_string_direct """
+        SELECT 
+            UNCOMPRESS(COMPRESS('Hello, world!')) AS decompressed_data
+        LIMIT 1;
+    """
+
+    // Test 11: Verify that COMPRESS and UNCOMPRESS work with a numeric value
+    order_qt_compress_numeric_direct """
+        SELECT 
+            UNCOMPRESS(COMPRESS('12345')) AS decompressed_data
+        LIMIT 1;
+    """
+}

## For function tests

Refer to regression-test/suites/query_p0/sql_functions/test_template_one_arg.groovy and the two/three_args.groovy templates, and add BE unit tests (BE-UT).
