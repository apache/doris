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

package org.apache.doris.nereids.trees.expressions.visitor;

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
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;

/**
 * visitor function for all table generating function.
 */
public interface TableGeneratingFunctionVisitor<R, C> {
    R visitTableGeneratingFunction(TableGeneratingFunction tableGeneratingFunction, C context);

    default R visitExplode(Explode explode, C context) {
        return visitTableGeneratingFunction(explode, context);
    }

    default R visitExplodeOuter(ExplodeOuter explodeOuter, C context) {
        return visitTableGeneratingFunction(explodeOuter, context);
    }

    default R visitExplodeMap(ExplodeMap explode, C context) {
        return visitTableGeneratingFunction(explode, context);
    }

    default R visitExplodeMapOuter(ExplodeMapOuter explodeOuter, C context) {
        return visitTableGeneratingFunction(explodeOuter, context);
    }

    default R visitExplodeJsonObject(ExplodeJsonObject explode, C context) {
        return visitTableGeneratingFunction(explode, context);
    }

    default R visitExplodeJsonObjectOuter(ExplodeJsonObjectOuter explodeOuter, C context) {
        return visitTableGeneratingFunction(explodeOuter, context);
    }

    default R visitExplodeNumbers(ExplodeNumbers explodeNumbers, C context) {
        return visitTableGeneratingFunction(explodeNumbers, context);
    }

    default R visitExplodeNumbersOuter(ExplodeNumbersOuter explodeNumbersOuter, C context) {
        return visitTableGeneratingFunction(explodeNumbersOuter, context);
    }

    default R visitExplodeBitmap(ExplodeBitmap explodeBitmap, C context) {
        return visitTableGeneratingFunction(explodeBitmap, context);
    }

    default R visitExplodeBitmapOuter(ExplodeBitmapOuter explodeBitmapOuter, C context) {
        return visitTableGeneratingFunction(explodeBitmapOuter, context);
    }

    default R visitExplodeSplit(ExplodeSplit explodeSplit, C context) {
        return visitTableGeneratingFunction(explodeSplit, context);
    }

    default R visitExplodeSplitOuter(ExplodeSplitOuter explodeSplitOuter, C context) {
        return visitTableGeneratingFunction(explodeSplitOuter, context);
    }

    default R visitExplodeJsonArrayInt(ExplodeJsonArrayInt explodeJsonArrayInt, C context) {
        return visitTableGeneratingFunction(explodeJsonArrayInt, context);
    }

    default R visitExplodeJsonArrayIntOuter(ExplodeJsonArrayIntOuter explodeJsonArrayIntOuter, C context) {
        return visitTableGeneratingFunction(explodeJsonArrayIntOuter, context);
    }

    default R visitExplodeJsonArrayDouble(ExplodeJsonArrayDouble explodeJsonArrayDouble, C context) {
        return visitTableGeneratingFunction(explodeJsonArrayDouble, context);
    }

    default R visitExplodeJsonArrayDoubleOuter(ExplodeJsonArrayDoubleOuter explodeJsonArrayDoubleOuter, C context) {
        return visitTableGeneratingFunction(explodeJsonArrayDoubleOuter, context);
    }

    default R visitExplodeJsonArrayString(ExplodeJsonArrayString explodeJsonArrayString, C context) {
        return visitTableGeneratingFunction(explodeJsonArrayString, context);
    }

    default R visitExplodeJsonArrayStringOuter(ExplodeJsonArrayStringOuter explodeJsonArrayStringOuter, C context) {
        return visitTableGeneratingFunction(explodeJsonArrayStringOuter, context);
    }

    default R visitExplodeJsonArrayJson(ExplodeJsonArrayJson explodeJsonArrayJson, C context) {
        return visitTableGeneratingFunction(explodeJsonArrayJson, context);
    }

    default R visitExplodeJsonArrayJsonOuter(ExplodeJsonArrayJsonOuter explodeJsonArrayJsonOuter, C context) {
        return visitTableGeneratingFunction(explodeJsonArrayJsonOuter, context);
    }

    default R visitPosExplode(PosExplode posExplode, C context) {
        return visitTableGeneratingFunction(posExplode, context);
    }

    default R visitPosExplodeOuter(PosExplodeOuter posExplodeOuter, C context) {
        return visitTableGeneratingFunction(posExplodeOuter, context);
    }
}
