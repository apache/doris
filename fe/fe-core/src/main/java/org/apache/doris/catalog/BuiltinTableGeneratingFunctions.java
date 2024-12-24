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

import org.apache.doris.nereids.trees.expressions.functions.generator.Explode;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeBitmap;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeBitmapOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonArrayDouble;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonArrayDoubleOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonArrayInt;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonArrayIntOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonArrayJson;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonArrayJsonOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonArrayString;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonArrayStringOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonObject;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeJsonObjectOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeMap;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeMapOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeNumbers;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeNumbersOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeSplit;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeSplitOuter;
import org.apache.doris.nereids.trees.expressions.functions.generator.PosExplode;
import org.apache.doris.nereids.trees.expressions.functions.generator.PosExplodeOuter;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Builtin table generating functions.
 *
 * Note: Please ensure that this class only has some lists and no procedural code.
 *       It helps to be clear and concise.
 */
public class BuiltinTableGeneratingFunctions implements FunctionHelper {
    public final List<TableGeneratingFunc> tableGeneratingFunctions = ImmutableList.of(
            tableGenerating(Explode.class, "explode"),
            tableGenerating(ExplodeOuter.class, "explode_outer"),
            tableGenerating(ExplodeMap.class, "explode_map"),
            tableGenerating(ExplodeMapOuter.class, "explode_map_outer"),
            tableGenerating(ExplodeJsonObject.class, "explode_json_object"),
            tableGenerating(ExplodeJsonObjectOuter.class, "explode_json_object_outer"),
            tableGenerating(ExplodeNumbers.class, "explode_numbers"),
            tableGenerating(ExplodeNumbersOuter.class, "explode_numbers_outer"),
            tableGenerating(ExplodeBitmap.class, "explode_bitmap"),
            tableGenerating(ExplodeBitmapOuter.class, "explode_bitmap_outer"),
            tableGenerating(ExplodeSplit.class, "explode_split"),
            tableGenerating(ExplodeSplitOuter.class, "explode_split_outer"),
            tableGenerating(ExplodeJsonArrayInt.class, "explode_json_array_int"),
            tableGenerating(ExplodeJsonArrayIntOuter.class, "explode_json_array_int_outer"),
            tableGenerating(ExplodeJsonArrayDouble.class, "explode_json_array_double"),
            tableGenerating(ExplodeJsonArrayDoubleOuter.class, "explode_json_array_double_outer"),
            tableGenerating(ExplodeJsonArrayString.class, "explode_json_array_string"),
            tableGenerating(ExplodeJsonArrayStringOuter.class, "explode_json_array_string_outer"),
            tableGenerating(ExplodeJsonArrayJson.class, "explode_json_array_json"),
            tableGenerating(ExplodeJsonArrayJsonOuter.class, "explode_json_array_json_outer"),
            tableGenerating(PosExplode.class, "posexplode"),
            tableGenerating(PosExplodeOuter.class, "posexplode_outer")
    );

    public static final BuiltinTableGeneratingFunctions INSTANCE = new BuiltinTableGeneratingFunctions();

    // Note: Do not add any code here!
    private BuiltinTableGeneratingFunctions() {}
}
