// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.analysis.FunctionArgs;
import com.baidu.palo.analysis.FunctionName;
import com.baidu.palo.analysis.HdfsURI;
import com.baidu.palo.thrift.TAggregateFunction;
import com.baidu.palo.thrift.TFunction;

import java.util.List;

/**
 * Internal representation of a UDA.
 */
public class Uda extends Function {
    private Type intermediateType_;

    // The symbol inside the binary at location_ that contains this particular.
    // They can be null if it is not required.
    private String updateFnSymbol_;
    private String initFnSymbol_;
    private String serializeFnSymbol_;
    private String mergeFnSymbol_;
    private String finalizeFnSymbol_;

    public Uda(long id, FunctionName fnName, FunctionArgs args, Type retType) {
        super(fnName, args.argTypes, retType, args.hasVarArgs);
    }

    public Uda(long id, FunctionName fnName, List<Type> argTypes, Type retType,
      Type intermediateType, HdfsURI location, String updateFnSymbol, String initFnSymbol,
      String serializeFnSymbol, String mergeFnSymbol, String finalizeFnSymbol) {
        super(fnName, argTypes, retType, false);
        setLocation(location);
        intermediateType_ = intermediateType;
        updateFnSymbol_ = updateFnSymbol;
        initFnSymbol_ = initFnSymbol;
        serializeFnSymbol_ = serializeFnSymbol;
        mergeFnSymbol_ = mergeFnSymbol;
        finalizeFnSymbol_ = finalizeFnSymbol;
    }

    public String getUpdateFnSymbol() {
        return updateFnSymbol_;
    }

    public void setUpdateFnSymbol(String fn) {
        updateFnSymbol_ = fn;
    }

    public String getInitFnSymbol() {
        return initFnSymbol_;
    }

    public void setInitFnSymbol(String fn) {
        initFnSymbol_ = fn;
    }

    public String getSerializeFnSymbol() {
        return serializeFnSymbol_;
    }

    public void setSerializeFnSymbol(String fn) {
        serializeFnSymbol_ = fn;
    }

    public String getMergeFnSymbol() {
        return mergeFnSymbol_;
    }

    public void setMergeFnSymbol(String fn) {
        mergeFnSymbol_ = fn;
    }

    public String getFinalizeFnSymbol() {
        return finalizeFnSymbol_;
    }

    public void setFinalizeFnSymbol(String fn) {
        finalizeFnSymbol_ = fn;
    }

    public Type getIntermediateType() {
        return intermediateType_;
    }

    public void setIntermediateType(Type t) {
        intermediateType_ = t;
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        TAggregateFunction uda = new TAggregateFunction();
        uda.setUpdate_fn_symbol(updateFnSymbol_);
        uda.setInit_fn_symbol(initFnSymbol_);
        if (serializeFnSymbol_ == null) {
            uda.setSerialize_fn_symbol(serializeFnSymbol_);
        }
        uda.setMerge_fn_symbol(mergeFnSymbol_);
        uda.setFinalize_fn_symbol(finalizeFnSymbol_);
        uda.setIntermediate_type(intermediateType_.toThrift());
        fn.setAggregate_fn(uda);
        return fn;
    }
}
