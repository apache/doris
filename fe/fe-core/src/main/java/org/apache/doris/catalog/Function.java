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
import com.google.gson.annotations.SerializedName;
import org.apache.commons.io.output.NullOutputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Base class for all functions.
 */
public class Function implements Writable {

    public enum NullableMode {
        // Whether output column is nullable is depend on the input column is nullable
        DEPEND_ON_ARGUMENT,
        // like 'str_to_date', 'cast', 'date_format' etc, the output column is nullable
        // depend on input content
        ALWAYS_NULLABLE,
        // like 'count', the output column is always not nullable
        ALWAYS_NOT_NULLABLE
    }

    // Function id, every function has a unique id. Now all built-in functions' id is 0
    @SerializedName("id")
    private long id = 0;
    // User specified function name e.g. "Add"
    @SerializedName("n")
    private FunctionName name;
    @SerializedName("rt")
    private Type retType;
    // Array of parameter types.  empty array if this function does not have parameters.
    @SerializedName("at")
    private Type[] argTypes;
    // If true, this function has variable arguments.
    // TODO: we don't currently support varargs with no fixed types. i.e. fn(...)
    @SerializedName("hva")
    private boolean hasVarArgs;

    // If true (default), this function is called directly by the user. For operators,
    // this is false. If false, it also means the function is not visible from
    // 'show functions'.
    @SerializedName("uv")
    private boolean userVisible;

    // Absolute path in HDFS for the binary that contains this function.
    // e.g. /udfs/udfs.jar
    @SerializedName("l")
    private URI location;
    @SerializedName("bt")
    private TFunctionBinaryType binaryType;

    @SerializedName("nm")
    protected NullableMode nullableMode = NullableMode.DEPEND_ON_ARGUMENT;

    protected boolean vectorized = true;

    // library's checksum to make sure all backends use one library to serve user's request
    @SerializedName("cs")
    protected String checksum = "";

    // If true, this function is global function
    protected boolean isGlobal = false;
    // If true, this function is table function, mainly used by java-udtf
    @SerializedName("isU")
    protected boolean isUDTFunction = false;
    // iff true, this udf function is static load, and BE need cache class load.
    @SerializedName("isS")
    protected boolean isStaticLoad = false;
    @SerializedName("eT")
    protected long expirationTime = 360; // default 6 hours;
    @SerializedName("rv")
    protected String runtimeVersion;
    @SerializedName("fc")
    protected String functionCode;

    // Only used for serialization
    protected Function() {
    }

    public Function(FunctionName name, List<Type> args, Type retType, boolean varArgs) {
        this(0, name, args, retType, varArgs, true, NullableMode.DEPEND_ON_ARGUMENT);
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
        this.isGlobal = other.isGlobal;
        this.isUDTFunction = other.isUDTFunction;
        this.isStaticLoad = other.isStaticLoad;
        this.expirationTime = other.expirationTime;
        this.runtimeVersion = other.runtimeVersion;
        this.functionCode = other.functionCode;
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

    public String getRuntimeVersion() {
        return runtimeVersion;
    }

    public void setRuntimeVersion(String runtimeVersion) {
        this.runtimeVersion = runtimeVersion;
    }

    public String getFunctionCode() {
        return functionCode;
    }

    public void setFunctionCode(String functionCode) {
        this.functionCode = functionCode;
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

    public boolean isIdentical(Function o) {
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
        fn.setIsUdtfFunction(isUDTFunction);
        fn.setIsStaticLoad(isStaticLoad);
        fn.setExpirationTime(expirationTime);
        return fn;
    }

    // Child classes must override this function.
    public String toSql(boolean ifNotExists) {
        return "";
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Text.writeString(output, GsonUtils.GSON.toJson(this));
    }

    public static Function read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), Function.class);
    }

    public void setNullableMode(NullableMode nullableMode) {
        this.nullableMode = nullableMode;
    }

    public NullableMode getNullableMode() {
        return nullableMode;
    }

    public void setUDTFunction(boolean isUDTFunction) {
        this.isUDTFunction = isUDTFunction;
    }

    public boolean isUDTFunction() {
        return this.isUDTFunction;
    }

    public void setStaticLoad(boolean isStaticLoad) {
        this.isStaticLoad = isStaticLoad;
    }

    public boolean isStaticLoad() {
        return this.isStaticLoad;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getExpirationTime() {
        return this.expirationTime;
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
}
