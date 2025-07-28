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

import org.apache.doris.nereids.parser.Origin;

import java.util.function.Supplier;

/**
 * This class is used to extend some thread local fields for Thread,
 * so we can access the thread fields faster than ThreadLocal
 */
public class MoreFieldsThread extends Thread {
    private static final ThreadLocal<Boolean> keepFunctionSignatureThreadLocal = new ThreadLocal<>();

    private Origin origin;
    private boolean keepFunctionSignature;

    public MoreFieldsThread() {
    }

    public MoreFieldsThread(Runnable target) {
        super(target);
    }

    public MoreFieldsThread(ThreadGroup group, Runnable target) {
        super(group, target);
    }

    public MoreFieldsThread(String name) {
        super(name);
    }

    public MoreFieldsThread(ThreadGroup group, String name) {
        super(group, name);
    }

    public MoreFieldsThread(Runnable target, String name) {
        super(target, name);
    }

    public MoreFieldsThread(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
    }

    public MoreFieldsThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, target, name, stackSize);
    }

    public final void setOrigin(Origin origin) {
        this.origin = origin;
    }

    public final Origin getOrigin() {
        return this.origin;
    }

    /** when keepFunctionSignature is true, we will use the origin function signature for the rewritten expression */
    public static boolean isKeepFunctionSignature() {
        Thread thread = Thread.currentThread();
        if (thread instanceof MoreFieldsThread) {
            return ((MoreFieldsThread) thread).keepFunctionSignature;
        } else {
            Boolean keep = MoreFieldsThread.keepFunctionSignatureThreadLocal.get();
            return keep != null && keep;
        }
    }

    public static <T> T withKeepFunctionSignature(Supplier<T> callback) {
        return withKeepFunctionSignature(true, callback);
    }

    /** when keepFunctionSignature is true, we will use the origin function signature for the rewritten expression */
    public static <T> T withKeepFunctionSignature(boolean keepFunctionSignature, Supplier<T> callback) {
        Thread thread = Thread.currentThread();
        if (thread instanceof MoreFieldsThread) {
            MoreFieldsThread moreFieldsThread = (MoreFieldsThread) thread;
            boolean originKeepFunctionSignature = moreFieldsThread.keepFunctionSignature;
            try {
                moreFieldsThread.keepFunctionSignature = keepFunctionSignature;
                return callback.get();
            } finally {
                moreFieldsThread.keepFunctionSignature = originKeepFunctionSignature;
            }
        } else {
            Boolean originKeepFunctionSignature = keepFunctionSignatureThreadLocal.get();
            try {
                keepFunctionSignatureThreadLocal.set(keepFunctionSignature);
                return callback.get();
            } finally {
                if (originKeepFunctionSignature != null) {
                    keepFunctionSignatureThreadLocal.set(originKeepFunctionSignature);
                } else {
                    keepFunctionSignatureThreadLocal.remove();
                }
            }
        }
    }
}
