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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.URI;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Base class for all functions.
 */
public class Function implements Writable {
    private static final Logger LOG = LogManager.getLogger(Function.class);

    // Enum for how to compare function signatures.
    // For decimal types, the type in the function can be a wildcard, i.e. decimal(*,*).
    // The wildcard can *only* exist as function type, the caller will always be a
    // fully specified decimal.
    // For the purposes of function type resolution, decimal(*,*) will match exactly
    // with any fully specified decimal (i.e. fn(decimal(*,*)) matches identically for
    // the call to fn(decimal(1,0)).
    public enum CompareMode {
        // Two signatures are identical if the number of arguments and their types match
        // exactly and either both signatures are varargs or neither.
        IS_IDENTICAL,

        // Two signatures are indistinguishable if there is no way to tell them apart
        // when matching a particular instantiation. That is, their fixed arguments
        // match exactly and the remaining varargs have the same type.
        // e.g. fn(int, int, int) and fn(int...)
        // Argument types that are NULL are ignored when doing this comparison.
        // e.g. fn(NULL, int) is indistinguishable from fn(int, int)
        IS_INDISTINGUISHABLE,

        // X is a supertype of Y if Y.arg[i] can be strictly implicitly cast to X.arg[i]. If
        /// X has vargs, the remaining arguments of Y must be strictly implicitly castable
        // to the var arg type. The key property this provides is that X can be used in place
        // of Y. e.g. fn(int, double, string...) is a supertype of fn(tinyint, float, string,
        // string)
        IS_SUPERTYPE_OF,

        // Nonstrict supertypes broaden the definition of supertype to accept implicit casts
        // of arguments that may result in loss of precision - e.g. decimal to float.
        IS_NONSTRICT_SUPERTYPE_OF,

        // Used to drop UDF. User can drop function through name or name and arguments.
        // If X is matchable with Y, this will only check X's element is identical with Y's.
        // e.g. fn is matchable with fn(int), fn(float) and fn(int) is only matchable with fn(int).
        IS_MATCHABLE
    }

    public enum NullableMode {
        // Whether output column is nullable is depend on the input column is nullable
        DEPEND_ON_ARGUMENT,
        // like 'str_to_date', 'cast', 'date_format' etc, the output column is nullable
        // depend on input content
        ALWAYS_NULLABLE,
        // like 'count', the output column is always not nullable
        ALWAYS_NOT_NULLABLE,
        // Whether output column is nullable is depend on custom algorithm by @Expr.isNullable()
        CUSTOM
    }

    public static final long UNIQUE_FUNCTION_ID = 0;
    // Function id, every function has a unique id. Now all built-in functions' id is 0
    @SerializedName(value = "id")
    private long id = 0;
    // User specified function name e.g. "Add"
    @SerializedName(value = "name")
    private FunctionName name;
    @SerializedName(value = "retType")
    private Type retType;
    // Array of parameter types.  empty array if this function does not have parameters.
    @SerializedName(value = "argTypes")
    private Type[] argTypes;
    // If true, this function has variable arguments.
    // TODO: we don't currently support varargs with no fixed types. i.e. fn(...)
    @SerializedName(value = "hasVarArgs")
    private boolean hasVarArgs;

    // If true (default), this function is called directly by the user. For operators,
    // this is false. If false, it also means the function is not visible from
    // 'show functions'.
    @SerializedName(value = "userVisible")
    private boolean userVisible;

    // Absolute path in HDFS for the binary that contains this function.
    // e.g. /udfs/udfs.jar
    @SerializedName(value = "location")
    private URI location;

    @SerializedName(value = "binaryType")
    private TFunctionBinaryType binaryType;

    @SerializedName(value = "nestedFunction")
    private Function nestedFunction = null;

    @SerializedName(value = "nullableMode")
    protected NullableMode nullableMode = NullableMode.DEPEND_ON_ARGUMENT;

    @SerializedName(value = "vectorized")
    protected boolean vectorized = true;

    // library's checksum to make sure all backends use one library to serve user's request
    @SerializedName(value = "checksum")
    protected String checksum = "";

    // If true, this function is global function
    @SerializedName(value = "isGlobal")
    protected boolean isGlobal = false;

    // Only used for serialization
    protected Function() {
    }

    public Function(FunctionName name, List<Type> args, Type retType, boolean varArgs) {
        this(0, name, args, retType, varArgs, true, NullableMode.DEPEND_ON_ARGUMENT);
    }

    public Function(FunctionName name, List<Type> args, Type retType, boolean varArgs, boolean vectorized) {
        this(0, name, args, retType, varArgs, vectorized, NullableMode.DEPEND_ON_ARGUMENT);
    }

    public Function(FunctionName name, List<Type> args, Type retType,
            boolean varArgs, boolean vectorized, NullableMode mode) {
        this(0, name, args, retType, varArgs, vectorized, mode);
    }

    public Function(long id, FunctionName name, List<Type> argTypes, Type retType, boolean hasVarArgs,
            TFunctionBinaryType binaryType, boolean userVisible, boolean vectorized, NullableMode mode) {
        this.id = id;
        this.name = name;
        this.hasVarArgs = hasVarArgs;
        if (argTypes.size() > 0) {
            this.argTypes = argTypes.toArray(new Type[argTypes.size()]);
        } else {
            this.argTypes = new Type[0];
        }
        this.retType = retType;
        this.binaryType = binaryType;
        this.userVisible = userVisible;
        this.vectorized = vectorized;
        this.nullableMode = mode;
    }

    public Function(long id, FunctionName name, List<Type> argTypes, Type retType,
            boolean hasVarArgs, boolean vectorized, NullableMode mode) {
        this(id, name, argTypes, retType, hasVarArgs, TFunctionBinaryType.BUILTIN, true, vectorized, mode);
    }

    public Function(Function other) {
        if (other == null) {
            return;
        }
        this.id = other.id;
        this.name = new FunctionName(other.name.getDb(), other.name.getFunction());
        this.hasVarArgs = other.hasVarArgs;
        this.retType = other.retType;
        this.userVisible = other.userVisible;
        this.nullableMode = other.nullableMode;
        this.vectorized = other.vectorized;
        this.binaryType = other.binaryType;
        this.location = other.location;
        if (other.argTypes != null) {
            this.argTypes = new Type[other.argTypes.length];
            System.arraycopy(other.argTypes, 0, this.argTypes, 0, other.argTypes.length);
        }
        this.checksum = other.checksum;
    }

    public void setNestedFunction(Function nestedFunction) {
        this.nestedFunction = nestedFunction;
    }

    public Function getNestedFunction() {
        return nestedFunction;
    }

    public Function clone() {
        return new Function(this);
    }

    public FunctionName getFunctionName() {
        return name;
    }

    public String functionName() {
        return name.getFunction();
    }

    public String dbName() {
        return name.getDb();
    }

    public Type getReturnType() {
        return retType;
    }

    public void setReturnType(Type type) {
        this.retType = type;
    }

    public Type[] getArgs() {
        return argTypes;
    }

    public void setArgs(List<Type> argTypes) {
        this.argTypes = argTypes.toArray(new Type[argTypes.size()]);
    }

    // Returns the number of arguments to this function.
    public int getNumArgs() {
        return argTypes.length;
    }

    public URI getLocation() {
        return location;
    }

    public void setLocation(URI loc) {
        location = loc;
    }

    public void setName(FunctionName name) {
        this.name = name;
    }

    public TFunctionBinaryType getBinaryType() {
        return binaryType;
    }

    public void setBinaryType(TFunctionBinaryType type) {
        binaryType = type;
    }

    public boolean hasVarArgs() {
        return hasVarArgs;
    }

    public boolean isUserVisible() {
        return userVisible;
    }

    public void setUserVisible(boolean userVisible) {
        this.userVisible = userVisible;
    }

    public Type getVarArgsType() {
        if (!hasVarArgs) {
            return Type.INVALID;
        }
        Preconditions.checkState(argTypes.length > 0);
        return argTypes[argTypes.length - 1];
    }

    public void setHasVarArgs(boolean v) {
        hasVarArgs = v;
    }

    public void setId(long functionId) {
        this.id = functionId;
    }

    public long getId() {
        return id;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public String getChecksum() {
        return checksum;
    }

    public boolean isGlobal() {
        return isGlobal;
    }

    public void setGlobal(boolean global) {
        isGlobal = global;
    }

    // TODO(cmy): Currently we judge whether it is UDF by wheter the 'location' is set.
    // Maybe we should use a separate variable to identify,
    // but additional variables need to modify the persistence information.
    public boolean isUdf() {
        return location != null;
    }

    // Returns a string with the signature in human readable format:
    // FnName(argtype1, argtyp2).  e.g. Add(int, int)
    public String signatureString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name.getFunction()).append("(").append(Joiner.on(", ").join(argTypes));
        if (hasVarArgs) {
            sb.append("...");
        }
        sb.append(")");
        return sb.toString();
    }

    // Compares this to 'other' for mode.
    public boolean compare(Function other, CompareMode mode) {
        switch (mode) {
            case IS_IDENTICAL:
                return isIdentical(other);
            case IS_INDISTINGUISHABLE:
                return isIndistinguishable(other);
            case IS_SUPERTYPE_OF:
                return isSubtype(other);
            case IS_NONSTRICT_SUPERTYPE_OF:
                return isAssignCompatible(other);
            case IS_MATCHABLE:
                return isMatchable(other);
            default:
                Preconditions.checkState(false);
                return false;
        }
    }

    /**
     * Returns true if 'this' is a supertype of 'other'. Each argument in other must
     * be implicitly castable to the matching argument in this.
     * TODO: look into how we resolve implicitly castable functions. Is there a rule
     * for "most" compatible or maybe return an error if it is ambiguous?
     */
    private boolean isSubtype(Function other) {
        if (!this.hasVarArgs && other.argTypes.length != this.argTypes.length) {
            return false;
        }
        if (this.hasVarArgs && other.argTypes.length < this.argTypes.length) {
            return false;
        }
        for (int i = 0; i < this.argTypes.length; ++i) {
            if (!Type.isImplicitlyCastable(other.argTypes[i], this.argTypes[i], true)) {
                return false;
            }
        }
        // Check trailing varargs.
        if (this.hasVarArgs) {
            for (int i = this.argTypes.length; i < other.argTypes.length; ++i) {
                if (!Type.isImplicitlyCastable(other.argTypes[i], getVarArgsType(), true)) {
                    return false;
                }
            }
        }
        return true;
    }

    // return true if 'this' is assign-compatible from 'other'.
    // Each argument in 'other' must be assign-compatible to the matching argument in 'this'.
    private boolean isAssignCompatible(Function other) {
        if (!this.hasVarArgs && other.argTypes.length != this.argTypes.length) {
            return false;
        }
        if (this.hasVarArgs && other.argTypes.length < this.argTypes.length) {
            return false;
        }
        for (int i = 0; i < this.argTypes.length; ++i) {
            if (!Type.canCastTo(other.argTypes[i], argTypes[i])) {
                return false;
            }
        }
        // Check trailing varargs.
        if (this.hasVarArgs) {
            for (int i = this.argTypes.length; i < other.argTypes.length; ++i) {
                if (!Type.canCastTo(other.argTypes[i], getVarArgsType())) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isMatchable(Function o) {
        if (!o.name.equals(name)) {
            return false;
        }
        if (argTypes != null) {
            if (o.argTypes.length != this.argTypes.length) {
                return false;
            }
            if (o.hasVarArgs != this.hasVarArgs) {
                return false;
            }
            for (int i = 0; i < this.argTypes.length; ++i) {
                if (!o.argTypes[i].matchesType(this.argTypes[i])) {
                    return false;
                }
            }
        }
        return true;

    }

    private boolean isIdentical(Function o) {
        if (!o.name.equals(name)) {
            return false;
        }
        if (o.argTypes.length != this.argTypes.length) {
            return false;
        }
        if (o.hasVarArgs != this.hasVarArgs) {
            return false;
        }
        for (int i = 0; i < this.argTypes.length; ++i) {
            if (!o.argTypes[i].matchesType(this.argTypes[i])) {
                return false;
            }
        }
        return true;
    }

    private boolean isIndistinguishable(Function o) {
        if (!o.name.equals(name)) {
            return false;
        }
        int minArgs = Math.min(o.argTypes.length, this.argTypes.length);
        // The first fully specified args must be identical.
        for (int i = 0; i < minArgs; ++i) {
            if (o.argTypes[i].isNull() || this.argTypes[i].isNull()) {
                continue;
            }
            if (!o.argTypes[i].matchesType(this.argTypes[i])) {
                return false;
            }
        }
        if (o.argTypes.length == this.argTypes.length) {
            return true;
        }

        if (o.hasVarArgs && this.hasVarArgs) {
            if (!o.getVarArgsType().matchesType(this.getVarArgsType())) {
                return false;
            }
            if (this.getNumArgs() > o.getNumArgs()) {
                for (int i = minArgs; i < this.getNumArgs(); ++i) {
                    if (this.argTypes[i].isNull()) {
                        continue;
                    }
                    if (!this.argTypes[i].matchesType(o.getVarArgsType())) {
                        return false;
                    }
                }
            } else {
                for (int i = minArgs; i < o.getNumArgs(); ++i) {
                    if (o.argTypes[i].isNull()) {
                        continue;
                    }
                    if (!o.argTypes[i].matchesType(this.getVarArgsType())) {
                        return false;
                    }
                }
            }
            return true;
        } else if (o.hasVarArgs) {
            // o has var args so check the remaining arguments from this
            if (o.getNumArgs() > minArgs) {
                return false;
            }
            for (int i = minArgs; i < this.getNumArgs(); ++i) {
                if (this.argTypes[i].isNull()) {
                    continue;
                }
                if (!this.argTypes[i].matchesType(o.getVarArgsType())) {
                    return false;
                }
            }
            return true;
        } else if (this.hasVarArgs) {
            // this has var args so check the remaining arguments from s
            if (this.getNumArgs() > minArgs) {
                return false;
            }
            for (int i = minArgs; i < o.getNumArgs(); ++i) {
                if (o.argTypes[i].isNull()) {
                    continue;
                }
                if (!o.argTypes[i].matchesType(this.getVarArgsType())) {
                    return false;
                }
            }
            return true;
        } else {
            // Neither has var args and the lengths don't match
            return false;
        }
    }

    public boolean isInferenceFunction() {
        for (Type arg : argTypes) {
            if (arg instanceof AnyType) {
                return true;
            }
        }
        return retType instanceof AnyType;
    }

    public TFunction toThrift(Type realReturnType, Type[] realArgTypes, Boolean[] realArgTypeNullables) {
        TFunction fn = new TFunction();
        fn.setSignature(signatureString());
        fn.setName(name.toThrift());
        fn.setBinaryType(binaryType);
        if (location != null) {
            fn.setHdfsLocation(location.getLocation());
        }
        // `realArgTypes.length != argTypes.length` is true iff this is an aggregation
        // function.
        // For aggregation functions, `argTypes` here is already its real type with true
        // precision and scale.
        if (realArgTypes.length != argTypes.length) {
            fn.setArgTypes(Type.toThrift(Lists.newArrayList(argTypes)));
        } else {
            fn.setArgTypes(Type.toThrift(Lists.newArrayList(argTypes), Lists.newArrayList(realArgTypes)));
        }

        // For types with different precisions and scales, return type only indicates a
        // type with default
        // precision and scale so we need to transform it to the correct type.
        if (realReturnType.typeContainsPrecision() || realReturnType.isAggStateType()) {
            fn.setRetType(realReturnType.toThrift());
        } else {
            fn.setRetType(getReturnType().toThrift());
        }
        fn.setHasVarArgs(hasVarArgs);
        // TODO: Comment field is missing?
        // fn.setComment(comment)
        fn.setId(id);
        if (!checksum.isEmpty()) {
            fn.setChecksum(checksum);
        }
        fn.setVectorized(vectorized);
        return fn;
    }

    // Child classes must override this function.
    public String toSql(boolean ifNotExists) {
        return "";
    }

    public static Function getFunction(List<Function> fns, Function desc, CompareMode mode) {
        if (fns == null) {
            return null;
        }
        // First check for identical
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_IDENTICAL)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_IDENTICAL) {
            return null;
        }

        // Next check for indistinguishable
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_INDISTINGUISHABLE)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_INDISTINGUISHABLE) {
            return null;
        }

        // Next check for strict supertypes
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_SUPERTYPE_OF)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_SUPERTYPE_OF) {
            return null;
        }
        // Finally check for non-strict supertypes
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF)) {
                return f;
            }
        }
        return null;
    }

    enum FunctionType {
        ORIGIN(0),
        SCALAR(1),
        AGGREGATE(2),
        ALIAS(3);

        @SerializedName(value = "code")
        private final int code;

        FunctionType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static FunctionType fromCode(int code) {
            switch (code) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
                case 0:
                    return ORIGIN;
                case 1:
                    return SCALAR;
                case 2:
                    return AGGREGATE;
                case 3:
                    return ALIAS;
            }
            return null;
        }

        @Deprecated
        public static FunctionType read(DataInput input) throws IOException {
            return fromCode(input.readInt());
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        throw new Error("Origin function cannot be serialized");
    }

    @Deprecated
    public void readFields(DataInput input) throws IOException {
        id = input.readLong();
        name = FunctionName.read(input);
        retType = ColumnType.read(input);
        int numArgs = input.readInt();
        argTypes = new Type[numArgs];
        for (int i = 0; i < numArgs; ++i) {
            argTypes[i] = ColumnType.read(input);
        }
        hasVarArgs = input.readBoolean();
        userVisible = input.readBoolean();
        binaryType = TFunctionBinaryType.findByValue(input.readInt());

        boolean hasLocation = input.readBoolean();
        if (hasLocation) {
            String locationStr = Text.readString(input);
            try {
                location = URI.create(locationStr);
            } catch (AnalysisException e) {
                LOG.warn("failed to parse location:" + locationStr);
            }

        }
        boolean hasChecksum = input.readBoolean();
        if (hasChecksum) {
            checksum = Text.readString(input);
        }
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_126) {
            nullableMode = NullableMode.valueOf(input.readUTF());
        }
    }

    public static Function read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_127) {
            Function function;
            FunctionType functionType = FunctionType.read(in);
            switch (functionType) {
                case SCALAR:
                    function = new ScalarFunction();
                    break;
                case AGGREGATE:
                    function = new AggregateFunction();
                    break;
                case ALIAS:
                    function = new AliasFunction();
                    break;
                default:
                    throw new Error("Unsupported function type, type=" + functionType);
            }
            function.readFields(in);
            return function;
        }
        String json = Text.readString(in);
        JsonObject jsonObject = GsonUtils.GSON.fromJson(json, JsonObject.class);
        FunctionType functionType = FunctionType.fromCode(
                jsonObject.getAsJsonObject("functionType")
                .get("code")
                .getAsInt());
        switch (functionType) {
            case SCALAR:
                return GsonUtils.GSON.fromJson(json, ScalarFunction.class);
            case AGGREGATE:
                return GsonUtils.GSON.fromJson(json, AggregateFunction.class);
            case ALIAS:
                return GsonUtils.GSON.fromJson(json, AliasFunction.class);
            default:
                throw new Error("Unsupported function type, type=" + functionType);
        }
    }

    public String getProperties() {
        return "";
    }

    public List<Comparable> getInfo(boolean isVerbose) {
        List<Comparable> row = Lists.newArrayList();
        if (isVerbose) {
            // signature
            row.add(signatureString());
            // return type
            row.add(getReturnType().getPrimitiveType().toString());
            // function type
            // intermediate type
            if (this instanceof ScalarFunction) {
                row.add("Scalar");
                row.add("NULL");
            } else if (this instanceof AliasFunction) {
                row.add("Alias");
                row.add("NULL");
            } else {
                row.add("Aggregate");
                AggregateFunction aggFunc = (AggregateFunction) this;
                Type intermediateType = aggFunc.getIntermediateType();
                if (intermediateType != null) {
                    row.add(intermediateType.getPrimitiveType().toString());
                } else {
                    row.add("NULL");
                }
            }
            // property
            row.add(getProperties());
        } else {
            row.add(functionName());
        }
        return row;
    }

    public void setNullableMode(NullableMode nullableMode) {
        this.nullableMode = nullableMode;
    }

    public NullableMode getNullableMode() {
        return nullableMode;
    }

    // Try to serialize this function and write to nowhere.
    // Just for checking if we forget to implement write() method for some Exprs.
    // To avoid FE exist when writing edit log.
    public void checkWritable() throws UserException {
        try {
            DataOutputStream out = new DataOutputStream(new NullOutputStream());
            write(out);
        } catch (Throwable t) {
            throw new UserException("failed to serialize function: " + functionName(), t);
        }
    }

    public boolean hasTemplateArg() {
        for (Type t : getArgs()) {
            if (t.hasTemplateType()) {
                return true;
            }
        }

        return false;
    }

    public boolean hasVariadicTemplateArg() {
        for (Type t : getArgs()) {
            if (t.needExpandTemplateType()) {
                return true;
            }
        }

        return false;
    }

    // collect expand size of variadic template
    public void collectTemplateExpandSize(Type[] args, Map<String, Integer> expandSizeMap) throws TypeException {
        for (int i = argTypes.length - 1; i >= 0; i--) {
            if (argTypes[i].hasTemplateType()) {
                if (argTypes[i].needExpandTemplateType()) {
                    argTypes[i].collectTemplateExpandSize(
                            Arrays.copyOfRange(args, i, args.length), expandSizeMap);
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Function function = (Function) o;
        return id == function.id && hasVarArgs == function.hasVarArgs && userVisible == function.userVisible
                && vectorized == function.vectorized && Objects.equals(name, function.name)
                && Objects.equals(retType, function.retType) && Arrays.equals(argTypes,
                function.argTypes) && Objects.equals(location, function.location)
                && binaryType == function.binaryType && nullableMode == function.nullableMode && Objects.equals(
                checksum, function.checksum);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, name, retType, hasVarArgs, userVisible, location, binaryType, nullableMode,
                vectorized, checksum);
        result = 31 * result + Arrays.hashCode(argTypes);
        return result;
    }

    public static FunctionCallExpr convertToStateCombinator(FunctionCallExpr fnCall) {
        Function aggFunction = fnCall.getFn();
        List<Type> arguments = Arrays.asList(aggFunction.getArgs());
        ScalarFunction fn = new ScalarFunction(
                new FunctionName(aggFunction.getFunctionName().getFunction() + Expr.AGG_STATE_SUFFIX), arguments,
                Expr.createAggStateType(aggFunction.getFunctionName().getFunction(),
                        fnCall.getChildren().stream().map(expr -> {
                            return expr.getType();
                        }).collect(Collectors.toList()), fnCall.getChildren().stream().map(expr -> {
                            return expr.isNullable();
                        }).collect(Collectors.toList())),
                aggFunction.hasVarArgs(), aggFunction.isUserVisible());
        fn.setNullableMode(NullableMode.ALWAYS_NOT_NULLABLE);
        fn.setBinaryType(TFunctionBinaryType.AGG_STATE);
        return new FunctionCallExpr(fn, fnCall.getParams());
    }

    public static FunctionCallExpr convertToMergeCombinator(FunctionCallExpr fnCall) {
        Function aggFunction = fnCall.getFn();
        aggFunction.setName(new FunctionName(aggFunction.getFunctionName().getFunction() + Expr.AGG_MERGE_SUFFIX));
        aggFunction.setArgs(Arrays.asList(fnCall.getChildren().get(0).getType()));
        aggFunction.setBinaryType(TFunctionBinaryType.AGG_STATE);
        return fnCall;
    }

    public static FunctionCallExpr convertToUnionCombinator(FunctionCallExpr fnCall) {
        Function aggFunction = fnCall.getFn();
        aggFunction.setName(new FunctionName(aggFunction.getFunctionName().getFunction() + Expr.AGG_UNION_SUFFIX));
        aggFunction.setArgs(Arrays.asList(fnCall.getChildren().get(0).getType()));
        aggFunction.setBinaryType(TFunctionBinaryType.AGG_STATE);
        aggFunction.setNullableMode(NullableMode.ALWAYS_NOT_NULLABLE);
        aggFunction.setReturnType(fnCall.getChildren().get(0).getType());
        fnCall.setType(fnCall.getChildren().get(0).getType());
        return fnCall;
    }
}
