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
import org.apache.doris.analysis.HdfsURI;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;


/**
 * Base class for all functions.
 */
public class Function {
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
    }

    public static final long UNIQUE_FUNCTION_ID = 0;
    // User specified function name e.g. "Add"
    private FunctionName name;
    private final Type retType;
    // Array of parameter types.  empty array if this function does not have parameters.
    private Type[] argTypes;
    // If true, this function has variable arguments.
    // TODO: we don't currently support varargs with no fixed types. i.e. fn(...)
    private boolean hasVarArgs;

    // If true (default), this function is called directly by the user. For operators,
    // this is false. If false, it also means the function is not visible from
    // 'show functions'.
    private boolean userVisible;

    // Absolute path in HDFS for the binary that contains this function.
    // e.g. /udfs/udfs.jar
    private HdfsURI location;
    private TFunctionBinaryType binaryType;

    public Function(FunctionName name, Type[] argTypes, Type retType, boolean varArgs) {
        this.name = name;
        this.hasVarArgs = varArgs;
        if (argTypes == null) {
            this.argTypes = new Type[0];
        } else {
            this.argTypes = argTypes;
        }
        this.retType = retType;
    }

    public Function(FunctionName name, List<Type> args, Type retType, boolean varArgs) {
        this(name, (Type[]) null, retType, varArgs);
        if (args.size() > 0) {
            argTypes = args.toArray(new Type[args.size()]);
        } else {
            argTypes = new Type[0];
        }
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

    public Type[] getArgs() {
        return argTypes;
    }

    // Returns the number of arguments to this function.
    public int getNumArgs() {
        return argTypes.length;
    }

    public HdfsURI getLocation() {
        return location;
    }

    public void setLocation(HdfsURI loc) {
        location = loc;
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

    public TFunction toThrift() {
        TFunction fn = new TFunction();
        fn.setSignature(signatureString());
        fn.setName(name.toThrift());
        fn.setBinary_type(binaryType);
        if (location != null) {
            fn.setHdfs_location(location.toString());
        }
        fn.setArg_types(Type.toThrift(argTypes));
        fn.setRet_type(getReturnType().toThrift());
        fn.setHas_var_args(hasVarArgs);
        // TODO: Comment field is missing?
        // fn.setComment(comment)
        return fn;
    }

    // Child classes must override this function.
    public String toSql(boolean ifNotExists) {
        return "";
    }

    public static String getUdfTypeName(PrimitiveType t) {
        switch (t) {
            case BOOLEAN:
                return "boolean_val";
            case TINYINT:
                return "tiny_int_val";
            case SMALLINT:
                return "small_int_val";
            case INT:
                return "int_val";
            case BIGINT:
                return "big_int_val";
            case LARGEINT:
                return "large_int_val";
            case FLOAT:
                return "float_val";
            case DOUBLE:
                return "double_val";
            case VARCHAR:
            case CHAR:
            case HLL:
                return "string_val";
            case DATE:
            case DATETIME:
                return "datetime_val";
            case DECIMAL:
                return "decimal_val";
            default:
                Preconditions.checkState(false, t.toString());
                return "";
        }
    }

    public static String getUdfType(PrimitiveType t) {
        switch (t) {
            case NULL_TYPE:
                return "AnyVal";
            case BOOLEAN:
                return "BooleanVal";
            case TINYINT:
                return "TinyIntVal";
            case SMALLINT:
                return "SmallIntVal";
            case INT:
                return "IntVal";
            case BIGINT:
                return "BigIntVal";
            case LARGEINT:
                return "LargeIntVal";
            case FLOAT:
                return "FloatVal";
            case DOUBLE:
                return "DoubleVal";
            case VARCHAR:
            case CHAR:
            case HLL:
                return "StringVal";
            case DATE:
            case DATETIME:
                return "DateTimeVal";
            case DECIMAL:
                return "DecimalVal";
            default:
                Preconditions.checkState(false, t.toString());
                return "";
        }
    }
}
