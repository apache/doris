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

package org.apache.doris.analysis;

/**
 * The OpcodeRegistry provides a mapping between function signatures and opcodes. The
 * supported functions are code-gen'ed and added to the registry with an assigned opcode.
 * The opcode is shared with the backend.  The frontend can use the registry to look up
 * a function's opcode.
 * <p/>
 * The OpcodeRegistry also contains a mapping between function names (as strings) to
 * operators.
 * <p/>
 * The OpcodeRegistry is a singleton.
 * <p/>
 * TODO: The opcode registry should be versioned in the FE/BE.
 */
public class OpcodeRegistry {
//
//    private final static Logger         LOG      = LogManager.getLogger(OpcodeRegistry.class);
//    private static       OpcodeRegistry instance = new OpcodeRegistry();
//    /**
//     * This is a mapping of Operator,#args to signatures with a fixed number of arguments.
//     * The signature is defined by the operator enum and the arguments
//     * and is a one to one mapping to opcodes.
//     * The map is structured this way to more efficiently look for signature matches.
//     * Signatures that have the same number of arguments have a potential to be matches
//     * by allowing types to be implicitly cast.
//     * Functions with a variable number of arguments are put into the varArgOperations map.
//     */
//    private final Map<Pair<FunctionOperator, Integer>, List<BuiltinFunction>> operations;
//    /**
//     * This is a mapping of Operator,varArgType to signatures of vararg functions only.
//     * varArgType must be a maximum-resolution type.
//     * We use a separate map to be able to support multiple vararg signatures for the same
//     * FunctionOperator.
//     * Contains a special entry mapping from Operator,NULL_TYPE to signatures for each
//     * Operator to correctly match varag functions when all args are NULL.
//     * Limitations: Since we do not consider the number of arguments, each FunctionOperator
//     * is limited to having one vararg signature per maximum-resolution PrimitiveType.
//     * For example, one can have two signatures func(float, int ...) and func(string ...),
//     * but not func(float, int ...) and func (int ...).
//     */
//    private final Map<Pair<FunctionOperator, PrimitiveType>, List<BuiltinFunction>>
//      varArgOperations;
//    /**
//     * This contains a mapping of function names to a FunctionOperator enum.  This is used
//     * by FunctionCallExpr to go from the parser input to function opcodes.
//     * This is a many to one mapping (i.e. substr and substring both map to the same
//     * operation).
//     * The mappings are filled in in FunctionRegistry.java which is auto-generated.
//     */
//    private final HashMap<String, FunctionOperator> functionNameMap;
//
//    private final HashMap<FunctionOperator, List<BuiltinFunction>> funcByOp;
//
//    // Singleton interface, don't call the constructor
//    private OpcodeRegistry() {
//        operations = Maps.newHashMap();
//        varArgOperations = Maps.newHashMap();
//        functionNameMap = Maps.newHashMap();
//        funcByOp = Maps.newHashMap();
//
//        // Add all the function signatures to the registry and the function name(string)
//        // to FunctionOperator mapping
//        FunctionRegistry.InitFunctions(this);
//    }
//
//    // Singleton interface
//    public static OpcodeRegistry instance() {
//        return instance;
//    }
//
//    /**
//     * Static utility functions
//     */
//    public static boolean isBitwiseOperation(FunctionOperator operator) {
//        return operator == FunctionOperator.BITAND || operator == FunctionOperator.BITNOT ||
//          operator == FunctionOperator.BITOR || operator == FunctionOperator.BITXOR;
//    }
//
//    /**
//     * Returns the set of function names.
//     *
//     * @return
//     */
//    public Set<String> getFunctionNames() {
//        return functionNameMap.keySet();
//    }
//
//    /**
//     * Returns the function operator enum.  The lookup is case insensitive.
//     * (i.e. "Substring" --> TExprOperator.STRING_SUBSTR).
//     * Returns INVALID_OP is that function name is unknown.
//     */
//    public FunctionOperator getFunctionOperator(String fnName) {
//        String lookup = fnName.toLowerCase();
//        if (functionNameMap.containsKey(lookup)) {
//            return functionNameMap.get(lookup);
//        }
//        return FunctionOperator.INVALID_OPERATOR;
//    }
//
//    /**
//     * Query for a function in the registry, specifying the operation, 'op', the arguments.
//     * If there is no matching signature, null will be returned.
//     * If there is a match, the matching signature will be returned.
//     * If 'allowImplicitCasts' is true the matching signature does not have to match the
//     * input identically, implicit type promotion is allowed.
//     */
//    public BuiltinFunction getFunctionInfo(
//            FunctionOperator op, boolean allowImplicitCasts,
//            boolean vectorFunction, PrimitiveType... argTypes) {
//        Pair<FunctionOperator, Integer> lookup = Pair.create(op, argTypes.length);
//        List<Pair<FunctionOperator, PrimitiveType>> varArgMatchTypes = null;
//        if (argTypes.length > 0) {
//            Set<PrimitiveType> maxResolutionTypes = getMaxResolutionTypes(argTypes);
//            Preconditions.checkNotNull(maxResolutionTypes);
//            varArgMatchTypes = Lists.newArrayList();
//            for (PrimitiveType maxResolutionType : maxResolutionTypes) {
//                varArgMatchTypes.add(Pair.create(op, maxResolutionType));
//            }
//        }
//        List<BuiltinFunction> functions = null;
//        if (operations.containsKey(lookup)) {
//            functions = operations.get(lookup);
//        } else if (!varArgMatchTypes.isEmpty()) {
//            functions = Lists.newArrayList();
//            List<BuiltinFunction> matchedFunctions = null;
//            for (Pair<FunctionOperator, PrimitiveType> varArgsMatchType : varArgMatchTypes) {
//                matchedFunctions = varArgOperations.get(varArgsMatchType);
//                if (matchedFunctions != null) {
//                    functions.addAll(matchedFunctions);
//                }
//            }
//        }
//
//        if (functions == null || functions.isEmpty()) {
//            // may be we can find from funByOp
//            if (funcByOp.containsKey(op)) {
//                functions = funcByOp.get(op);
//            } else {
//                return null;
//            }
//        }
//        Type[] args = new Type[argTypes.length];
//        int i = 0;
//        for (PrimitiveType type : argTypes) {
//            args[i] = Type.fromPrimitiveType(type);
//            i ++;
//        }
//        BuiltinFunction search = new BuiltinFunction(op, args);
//
//        BuiltinFunction compatibleMatch = null;
//        List<BuiltinFunction> compatibleMatchFunctions = Lists.newArrayList();
//        // We firstly choose functions using IS_SUBTYPE(only check cast-method is implemented),
//        // if more than one functions are found, give priority to the assign-copatible one.
//        for (BuiltinFunction function : functions) {
//            if (function.compare(search, Function.CompareMode.IS_INDISTINGUISHABLE)) {
//                if (vectorFunction == function.vectorFunction) {
//                    return function;
//                }
//            } else if (allowImplicitCasts
//                    && function.compare(search, Function.CompareMode.IS_SUPERTYPE_OF)) {
//                if (vectorFunction == function.vectorFunction) {
//                    compatibleMatchFunctions.add(function);
//                }
//            }
//        }
//
//        // If there are many compatible functions, we priority to choose the non-loss-precision one.
//        for (BuiltinFunction function : compatibleMatchFunctions) {
//            if (function.compare(search, Function.CompareMode.IS_SUPERTYPE_OF)) {
//                compatibleMatch = function;
//            } else {
//                LOG.info(" false {} {}", function.getReturnType(), function.getArgs());
//            }
//        }
//        if (compatibleMatch == null && compatibleMatchFunctions.size() > 0) {
//            compatibleMatch = compatibleMatchFunctions.get(0);
//        }
//
//        return compatibleMatch;
//    }
//
//    /**
//     * Returns the max resolution type for each argType that is not a NULL_TYPE. If all
//     * argument types are NULL_TYPE then a set will be returned containing NULL_TYPE.
//     */
//    private Set<PrimitiveType> getMaxResolutionTypes(PrimitiveType[] argTypes) {
//        Set<PrimitiveType> maxResolutionTypes = Sets.newHashSet();
//        for (int i = 0; i < argTypes.length; ++i) {
//            if (!argTypes[i].isNull()) {
//                maxResolutionTypes.add(argTypes[i].getMaxResolutionType());
//            }
//        }
//        if (maxResolutionTypes.isEmpty()) {
//            maxResolutionTypes.add(PrimitiveType.NULL_TYPE);
//        }
//        return maxResolutionTypes;
//    }
//
//    /**
//     * Add a function with the specified opcode/signature to the registry.
//     */
//
//    public boolean add(boolean udfInterface, boolean vectorFunction, FunctionOperator op,
//                       TExprOpcode opcode, boolean varArgs, PrimitiveType retType, PrimitiveType... args) {
//        List<BuiltinFunction> functions;
//        Pair<FunctionOperator, Integer> lookup = Pair.create(op, args.length);
//        // Take the last argument's type as the vararg type.
//        Pair<FunctionOperator, PrimitiveType> varArgsLookup = null;
//        // Special signature for vararg functions to handle matching when all args are NULL.
//        Pair<FunctionOperator, PrimitiveType> varArgsNullLookup = null;
//        Preconditions.checkArgument((varArgs) ? args.length > 0 : true);
//        if (varArgs && args.length > 0) {
//            varArgsLookup = Pair.create(op, args[args.length - 1].getMaxResolutionType());
//            varArgsNullLookup = Pair.create(op, PrimitiveType.NULL_TYPE);
//        }
//        if (operations.containsKey(lookup)) {
//            functions = operations.get(lookup);
//        } else if (varArgsLookup != null && varArgOperations.containsKey(varArgsLookup)) {
//            functions = varArgOperations.get(varArgsLookup);
//        } else {
//            functions = new ArrayList<BuiltinFunction>();
//            if (varArgs) {
//                varArgOperations.put(varArgsLookup, functions);
//                varArgOperations.put(varArgsNullLookup, functions);
//            } else {
//                operations.put(lookup, functions);
//            }
//        }
//
//        Type[] argsType = new Type[args.length];
//        int i = 0;
//        for (PrimitiveType type : args) {
//            argsType[i] = Type.fromPrimitiveType(type);
//            i ++;
//        }
//
//        BuiltinFunction function =
//                new BuiltinFunction(udfInterface, vectorFunction, opcode, op, varArgs, Type.fromPrimitiveType(retType), argsType);
//        if (functions.contains(function)) {
//            LOG.error("OpcodeRegistry: Function already exists: " + opcode);
//            return false;
//        }
//        functions.add(function);
//
//        // add to op map
//        if (funcByOp.containsKey(op)) {
//            functions = funcByOp.get(op);
//        } else {
//            functions = Lists.newArrayList();
//            funcByOp.put(op, functions);
//        }
//        functions.add(function);
//        return true;
//    }
//
//    public boolean addFunctionMapping(String functionName, FunctionOperator op) {
//        if (functionNameMap.containsKey(functionName)) {
//            LOG.error("OpcodeRegistry: Function mapping already exists: " + functionName);
//            return false;
//        }
//        functionNameMap.put(functionName, op);
//        return true;
//    }
//
//    /**
//     * Contains all the information about a builtin function.
//     * TODO: merge with Function and Udf
//     */
//    public static class BuiltinFunction extends Function {
//        // If true, this builtin is implemented against the Udf interface.
//        public final boolean          udfInterface;
//        public final boolean          vectorFunction;
//        public       TExprOpcode      opcode;
//        public       FunctionOperator operator;
//
//        // Constructor for searching, specifying the op and arguments
//        public BuiltinFunction(FunctionOperator operator, Type[] args) {
//            super(new FunctionName(operator.toString()), args, Type.INVALID, false);
//            this.operator = operator;
//            this.udfInterface = false;
//            this.vectorFunction = false;
//            this.setBinaryType(TFunctionBinaryType.BUILTIN);
//        }
//
//        private BuiltinFunction(boolean udfInterface, boolean vectorFunction, TExprOpcode opcode,
//                                FunctionOperator operator, boolean varArgs, Type ret, Type[] args) {
//            super(new FunctionName(opcode.toString()), args, ret, varArgs);
//            this.operator = operator;
//            this.opcode = opcode;
//            this.udfInterface = udfInterface;
//            this.vectorFunction = vectorFunction;
//            this.setBinaryType(TFunctionBinaryType.BUILTIN);
//        }
//    }
}
