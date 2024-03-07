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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/objects/MethodParams.java
// and modified by Doris

package org.apache.doris.plsql.objects;

import org.apache.doris.plsql.File;
import org.apache.doris.plsql.Row;
import org.apache.doris.plsql.Var;
import org.apache.doris.plsql.exception.ArityException;
import org.apache.doris.plsql.exception.TypeException;

import java.util.List;

public class MethodParams {
    private final List<Var> actual;

    public MethodParams(String methodName, List<Var> actual, Arity arity) {
        this.actual = actual;
        arity.check(methodName, actual);
    }

    public Long longAt(int nth) {
        return at(nth, Long.class);
    }

    public Row rowAt(int nth) {
        return at(nth, Row.class);
    }

    public String stringAt(int nth) {
        return at(nth, String.class);
    }

    public File fileAt(int nth) {
        return at(nth, File.class);
    }

    public <T> T at(int nth, Class<T> clazz) {
        try {
            return clazz.cast(actual.get(nth).value);
        } catch (ClassCastException e) {
            throw new TypeException(null, clazz, actual.get(nth).type, actual.get(nth).value);
        }
    }

    public interface Arity {
        void check(String methodName, List<?> params);

        Arity NULLARY = Arity.of(0);
        Arity UNARY = Arity.of(1);
        Arity BINARY = Arity.of(2);

        static Arity of(int count) {
            return (methodName, params) -> {
                if (params.size() != count) {
                    throw new ArityException(null, methodName, count, params.size());
                }
            };
        }

        static Arity min(int count) {
            return (methodName, params) -> {
                if (params.size() < count) {
                    throw new ArityException(null, "wrong number of arguments in call to '" + methodName
                            + "'. Expected at least " + count + " got " + params.size() + ".");
                }
            };
        }

        static Arity max(int count) {
            return (methodName, params) -> {
                if (params.size() > count) {
                    throw new ArityException(null, "wrong number of arguments in call to '" + methodName
                            + "'. Expected at most " + count + " got " + params.size() + ".");
                }
            };
        }
    }
}
