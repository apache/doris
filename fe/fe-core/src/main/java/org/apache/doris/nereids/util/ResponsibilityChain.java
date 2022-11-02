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

package org.apache.doris.nereids.util;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * ResponsibilityChain.
 *
 * e.g.
 * <pre>
 *     ResponsibilityChain.from(defaultResult)
 *          .then(computeA)
 *          .then(computeB)
 *          .then(computeC)
 *          .get();
 * </pre>
 * @param <T> input type.
 */
public class ResponsibilityChain<T> {
    private T defaultResult;
    private boolean assertNotNull;
    private List<Function<T, T>> chain = Lists.newArrayList();

    private ResponsibilityChain(T defaultResult) {
        this.defaultResult = defaultResult;
    }

    /**
     * start to compute child by the input parameter.
     * @param <T> result type.
     * @param defaultResult defaultResult
     * @return result
     */
    public static <T> ResponsibilityChain<T> from(T defaultResult) {
        return new ResponsibilityChain(defaultResult);
    }

    public ResponsibilityChain<T> assertNotNull() {
        this.assertNotNull = true;
        return this;
    }

    /**
     * add a computed logic to the chain.
     * @param computeFunction consume previous result, return new result
     * @return this.
     */
    public ResponsibilityChain<T> then(Function<T, T> computeFunction) {
        chain.add(computeFunction);
        return this;
    }

    /**
     * add a computed logic to the chain if you not care about the previous result.
     * @param computeFunction return new result
     * @return this.
     */
    public ResponsibilityChain<T> then(Supplier<T> computeFunction) {
        chain.add(previousResult -> computeFunction.get());
        return this;
    }

    /**
     * get result.
     * @return result
     */
    public T get() {
        T result = defaultResult;
        for (Function<T, T> branchFunction : chain) {
            result = branchFunction.apply(result);
        }
        if (result == null) {
            throw new NullPointerException("Result can not be null");
        }
        return result;
    }
}
