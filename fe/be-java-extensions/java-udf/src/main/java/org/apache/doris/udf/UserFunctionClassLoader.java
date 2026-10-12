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

package org.apache.doris.udf;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * A user function's own jars, with {@link UdfContractClassLoader} behind them.
 *
 * <p>Ordinary {@link URLClassLoader} behaviour plus one thing: the class that could not be found
 * anywhere is reported with what the search covered. This is the loader that has to say it -
 * {@code ClassLoader.loadClass} discards whatever the parent threw and reports its own miss, so a
 * message attached further up never reaches anyone.
 */
final class UserFunctionClassLoader extends URLClassLoader {

    static {
        registerAsParallelCapable();
    }

    UserFunctionClassLoader(URL[] urls, ClassLoader contract) {
        super(urls, contract);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        try {
            return super.findClass(name);
        } catch (ClassNotFoundException e) {
            throw new ClassNotFoundException(name + ". A Java function runs in isolation: it sees the"
                    + " JDK, the jar it was created from, and the Hive UDF and joda date classes Doris"
                    + " publishes, and nothing else Doris happens to ship. Bundle this class into that"
                    + " jar.", e);
        }
    }
}
