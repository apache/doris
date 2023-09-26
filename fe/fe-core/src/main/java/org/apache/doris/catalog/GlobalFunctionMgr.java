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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * GlobalFunctionMgr will load all global functions at FE startup.
 * Provides management of global functions such as add, drop and other operations
 */
public class GlobalFunctionMgr extends MetaObject {
    private static final Logger LOG = LogManager.getLogger(GlobalFunctionMgr.class);

    // user define function
    private ConcurrentMap<String, ImmutableList<Function>> name2Function = Maps.newConcurrentMap();

    public static GlobalFunctionMgr read(DataInput in) throws IOException {
        GlobalFunctionMgr globalFunctionMgr = new GlobalFunctionMgr();
        globalFunctionMgr.readFields(in);
        return globalFunctionMgr;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        // write functions
        FunctionUtil.write(out, name2Function);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        FunctionUtil.readFields(in, null, name2Function);
    }

    public synchronized void addFunction(Function function, boolean ifNotExists) throws UserException {
        function.setGlobal(true);
        function.checkWritable();
        if (FunctionUtil.addFunctionImpl(function, ifNotExists, false, name2Function)) {
            Env.getCurrentEnv().getEditLog().logAddGlobalFunction(function);
            try {
                FunctionUtil.translateToNereids(null, function);
            } catch (Exception e) {
                LOG.warn("Nereids add function failed", e);
            }
        }
    }


    public synchronized void replayAddFunction(Function function) {
        try {
            function.setGlobal(true);
            FunctionUtil.addFunctionImpl(function, false, true, name2Function);
            FunctionUtil.translateToNereids(null, function);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void dropFunction(FunctionSearchDesc function, boolean ifExists) throws UserException {
        if (FunctionUtil.dropFunctionImpl(function, ifExists, name2Function)) {
            Env.getCurrentEnv().getEditLog().logDropGlobalFunction(function);
            FunctionUtil.dropFromNereids(null, function);
        }
    }

    public synchronized void replayDropFunction(FunctionSearchDesc functionSearchDesc) {
        try {
            FunctionUtil.dropFunctionImpl(functionSearchDesc, false, name2Function);
            FunctionUtil.dropFromNereids(null, functionSearchDesc);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }


    public synchronized Function getFunction(Function desc, Function.CompareMode mode) {
        return FunctionUtil.getFunction(desc, mode, name2Function);
    }

    public synchronized Function getFunction(FunctionSearchDesc function) throws AnalysisException {
        return FunctionUtil.getFunction(function, name2Function);
    }

    public synchronized List<Function> getFunctions() {
        return FunctionUtil.getFunctions(name2Function);
    }

}
