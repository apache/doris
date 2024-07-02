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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * SearchSignature. search candidate signature by the argument's type and predicate strategy,
 * e.g. IdenticalSignature, NullOrIdenticalSignature, ImplicitlyCastableSignature, AssignCompatibleSignature.
 */
public class SearchSignature {

    private final ComputeSignature computeSignature;
    private final List<FunctionSignature> signatures;
    private final List<Expression> arguments;

    // param1: signature type
    // param2: real argument type
    // return: is the real argument type matches the signature type?
    private final List<BiFunction<DataType, DataType, Boolean>> typePredicatePerRound
            = Lists.newArrayList();

    private SearchSignature(ComputeSignature computeSignature,
            List<FunctionSignature> signatures, List<Expression> arguments) {
        this.computeSignature = computeSignature;
        this.signatures = signatures;
        this.arguments = arguments;
    }

    public static SearchSignature from(ComputeSignature computeSignature,
            List<FunctionSignature> signatures, List<Expression> arguments) {
        return new SearchSignature(computeSignature, signatures, arguments);
    }

    public SearchSignature orElseSearch(BiFunction<DataType, DataType, Boolean> typePredicate) {
        typePredicatePerRound.add(typePredicate);
        return this;
    }

    /**
     * result.
     * @return Optional functionSignature result
     */
    public Optional<FunctionSignature> result() {
        // search every round
        for (BiFunction<DataType, DataType, Boolean> typePredicate : typePredicatePerRound) {
            int candidateNonStrictMatched = Integer.MAX_VALUE;
            int candidateDateToDateV2Count = Integer.MIN_VALUE;
            FunctionSignature candidate = null;
            for (FunctionSignature signature : signatures) {
                if (doMatchArity(signature, arguments) && doMatchTypes(signature, arguments, typePredicate)) {
                    // first we need to check decimal v3 precision promotion
                    if (computeSignature instanceof ComputePrecision) {
                        if (!((ComputePrecision) computeSignature).checkPrecision(signature)) {
                            continue;
                        }
                    } else {
                        // default check
                        if (!checkDecimalV3Precision(signature)) {
                            continue;
                        }
                    }
                    // has most identical matched signature has the highest priority
                    Pair<Integer, Integer> currentNonStrictMatched = nonStrictMatchedCount(signature, arguments);
                    if (currentNonStrictMatched.first < candidateNonStrictMatched) {
                        candidateNonStrictMatched = currentNonStrictMatched.first;
                        candidateDateToDateV2Count = currentNonStrictMatched.second;
                        candidate = signature;
                    } else if (currentNonStrictMatched.first == candidateNonStrictMatched) {
                        // if we need to do same count cast, then we choose the signature need to do more v1 to v2 cast
                        if (candidateDateToDateV2Count < currentNonStrictMatched.second) {
                            candidateDateToDateV2Count = currentNonStrictMatched.second;
                            candidate = signature;
                        }
                    }
                }
            }
            if (candidate != null) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    /**
     * get the result, throw can not found function if no result.
     * @param functionName the function name.
     * @return the result.
     */
    public FunctionSignature resultOrException(String functionName) {
        Optional<FunctionSignature> result = result();
        if (!result.isPresent()) {
            throwCanNotFoundFunctionException(functionName, arguments);
        }
        return result.get();
    }

    /**
     * default decimalv3 precision check, use wider type for all decimalv3 input.
     */
    private boolean checkDecimalV3Precision(FunctionSignature signature) {
        DataType finalType = null;
        for (int i = 0; i < arguments.size(); i++) {
            DataType targetType;
            if (i >= signature.argumentsTypes.size()) {
                if (signature.getVarArgType().isPresent()) {
                    targetType = signature.getVarArgType().get();
                } else {
                    return false;
                }
            } else {
                targetType = signature.getArgType(i);
            }
            if (!targetType.isDecimalV3Type()) {
                continue;
            }
            if (finalType == null) {
                finalType = DecimalV3Type.forType(arguments.get(i).getDataType());
            } else {
                Expression arg = arguments.get(i);
                if (arg.isLiteral() && arg.getDataType().isIntegralType()) {
                    // create decimalV3 with minimum scale enough to hold the integral literal
                    finalType = DecimalV3Type.createDecimalV3Type(new BigDecimal(((Literal) arg).getStringValue()));
                } else {
                    finalType = DecimalV3Type.widerDecimalV3Type((DecimalV3Type) finalType,
                            DecimalV3Type.forType(arg.getDataType()), true);
                }
            }
            if (!finalType.isDecimalV3Type()) {
                return false;
            }
        }
        return true;
    }

    private boolean doMatchArity(FunctionSignature sig, List<Expression> arguments) {
        int realArity = arguments.size();
        if (sig.hasVarArgs && sig.arity > realArity) {
            return false;
        } else if (!sig.hasVarArgs && sig.arity != realArity) {
            return false;
        }
        return true;
    }

    /**
     * return non-strict matched count and convert v1 to v2 count.
     *
     * @return the first value indic non-strict matched count, the second value indic date to datev2 count
     */
    private Pair<Integer, Integer> nonStrictMatchedCount(FunctionSignature sig, List<Expression> arguments) {
        int nonStrictMatched = 0;
        int dateToDateV2Count = 0;
        int arity = arguments.size();
        for (int i = 0; i < arity; i++) {
            DataType sigArgType = sig.getArgType(i);
            DataType realType = arguments.get(i).getDataType();
            if (!IdenticalSignature.isIdentical(sigArgType, realType)) {
                nonStrictMatched++;
                if (sigArgType instanceof DateV2Type && realType instanceof DateType) {
                    dateToDateV2Count++;
                } else if (sigArgType instanceof DateTimeV2Type && (realType instanceof DateTimeType
                        || realType instanceof DateV2Type || realType instanceof DateType)) {
                    dateToDateV2Count++;
                }
            }
        }
        return Pair.of(nonStrictMatched, dateToDateV2Count);
    }

    private boolean doMatchTypes(FunctionSignature sig, List<Expression> arguments,
            BiFunction<DataType, DataType, Boolean> typePredicate) {
        int arity = arguments.size();
        for (int i = 0; i < arity; i++) {
            DataType sigArgType = sig.getArgType(i);
            DataType realType = arguments.get(i).getDataType();
            // we need to try to do string literal coercion when search signature.
            // for example, FUNC_A has two signature FUNC_A(datetime) and FUNC_A(string)
            // if SQL block is `FUNC_A('2020-02-02 00:00:00')`, we should return signature FUNC_A(datetime).
            if (arguments.get(i).isLiteral() && realType.isStringLikeType()) {
                realType = TypeCoercionUtils.characterLiteralTypeCoercion(((Literal) arguments.get(i)).getStringValue(),
                        sigArgType).orElse(arguments.get(i)).getDataType();
            }
            if (!typePredicate.apply(sigArgType, realType)) {
                return false;
            }
        }
        return true;
    }

    public static void throwCanNotFoundFunctionException(String name, List<Expression> arguments) {
        String missingSignature = name + arguments.stream()
                .map(Expression::getDataType)
                .map(DataType::toSql)
                .collect(Collectors.joining(", ", "(", ")"));
        throw new AnalysisException("Can not find the compatibility function signature: " + missingSignature);
    }
}
