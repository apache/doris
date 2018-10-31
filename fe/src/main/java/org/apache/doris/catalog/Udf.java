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

package org.apache.doris.catalog;

import org.apache.doris.analysis.FunctionArgs;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.HdfsURI;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TScalarFunction;

import java.util.List;


/**
 * Internal representation of a UDF.
 * TODO: unify this with builtins.
 */

public class Udf extends Function {
    // The name inside the binary at location_ that contains this particular
    // UDF. e.g. org.example.MyUdf.class.
    private String symbolName_;

    public Udf(long id, FunctionName fnName, FunctionArgs args, Type retType) {
       super(fnName, args.argTypes, retType, args.hasVarArgs);
    }

    public Udf(long id, FunctionName fnName, List<Type> argTypes, Type retType,
      HdfsURI location, String symbolName) {
        super(fnName, argTypes, retType, false);
        setLocation(location);
        setSymbolName(symbolName);
    }

    public String getSymbolName() {
        return symbolName_;
    }

    public void setSymbolName(String s) {
        symbolName_ = s;
    }

    @Override
    public TFunction toThrift() {
        TFunction fn = super.toThrift();
        fn.setScalar_fn(new TScalarFunction());
        fn.getScalar_fn().setSymbol(symbolName_);
        return fn;
    }
}
