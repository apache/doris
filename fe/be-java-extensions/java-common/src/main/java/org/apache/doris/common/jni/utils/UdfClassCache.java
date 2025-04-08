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

package org.apache.doris.common.jni.utils;

import com.esotericsoftware.reflectasm.MethodAccess;

import java.lang.reflect.Method;
import java.util.HashMap;

/**
 * This class is used for caching the class of UDF.
 */
public class UdfClassCache {
    public Class<?> udfClass;
    // the index of evaluate() method in the class
    public MethodAccess methodAccess;
    // the argument and return's JavaUdfDataType of evaluate() method.
    public JavaUdfDataType[] argTypes;
    // the class type of the arguments in evaluate() method
    public Class[] argClass;
    // The return type class of evaluate() method
    public JavaUdfDataType retType;
    public Class retClass;

    // all methods in the class for java-udf/ java-udaf
    public HashMap<String, Method> allMethods;
    // for java-udf  index is evaluate method index
    // for java-udaf index is add method index
    public int methodIndex;
}
