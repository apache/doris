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

package org.apache.doris.connector.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Enforces that {@link ForwardingConnectorContext} forwards EVERY {@link ConnectorContext} method.
 *
 * <p><b>WHY this matters:</b> the two classloader-pinning decorators (iceberg, paimon) exist only to keep
 * reflective loads inside their plugin's classloader. They used to implement {@link ConnectorContext}
 * directly and copy each method by hand, which fails open: nearly every method on that interface has a
 * default implementation whose semantics are a silent downgrade — {@code getFileSystem} returns
 * {@code null}, {@code executeAuthenticated} runs the task with no authentication, and
 * {@code newStorageUriNormalizer} discards the per-scan memoization. Miss one and the compiler says
 * nothing; the call simply stops reaching the engine, and for a pinning decorator it also stops being
 * pinned. That is not hypothetical: iceberg had lost {@code getFileSystem}, paimon had lost
 * {@code getFileSystem} and {@code newStorageUriNormalizer}.
 *
 * <p>So this test does not check a list someone has to remember to update — it reflects over the
 * interface and requires every method to arrive at the wrapped context, which makes adding a method to
 * {@link ConnectorContext} without a matching forward a build failure.
 */
public class ForwardingConnectorContextTest {

    /** Records the exact method each call landed on, so a forward can be checked one-for-one. */
    private static final class Recorder implements InvocationHandler {
        private final List<Method> calls = new ArrayList<>();

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            calls.add(method);
            return defaultValue(method.getReturnType());
        }
    }

    private static Object defaultValue(Class<?> type) {
        if (type == void.class) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == String.class) {
            return "";
        }
        if (type == Map.class) {
            return Collections.emptyMap();
        }
        if (type == List.class) {
            return Collections.emptyList();
        }
        if (type.isInterface()) {
            return Proxy.newProxyInstance(ForwardingConnectorContextTest.class.getClassLoader(),
                    new Class<?>[] {type}, (p, m, a) -> null);
        }
        return null;
    }

    private static Object[] argsFor(Method method) {
        Class<?>[] types = method.getParameterTypes();
        Object[] args = new Object[types.length];
        for (int i = 0; i < types.length; i++) {
            if (types[i] == Callable.class) {
                args[i] = (Callable<Object>) () -> null;
            } else if (types[i] == String.class) {
                args[i] = "x";
            } else if (types[i] == Map.class) {
                args[i] = Collections.emptyMap();
            } else if (types[i] == List.class) {
                args[i] = Collections.emptyList();
            } else if (types[i] == int.class) {
                args[i] = 0;
            } else if (types[i] == long.class) {
                args[i] = 0L;
            } else if (types[i] == boolean.class) {
                args[i] = false;
            } else {
                args[i] = null;
            }
        }
        return args;
    }

    @Test
    public void everyContextMethodReachesTheWrappedContext() throws Exception {
        for (Method method : ConnectorContext.class.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            Recorder recorder = new Recorder();
            ConnectorContext wrapped = (ConnectorContext) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {ConnectorContext.class}, recorder);
            ConnectorContext forwarding = new ForwardingConnectorContext(wrapped) {
            };

            method.invoke(forwarding, argsFor(method));

            Assertions.assertEquals(1, recorder.calls.size(),
                    method.getName() + " did not reach the wrapped context exactly once - "
                            + "ForwardingConnectorContext is missing a forward for it. Add one. If the "
                            + "method can run plugin code, the pinning subclasses (iceberg/paimon "
                            + "TcclPinningConnectorContext) must also override it and apply their pin: "
                            + "this base class only guarantees no call is LOST, not that it is pinned.");
            // Compare on name AND parameter types: several methods here are overloads of each other, and
            // an interface default that quietly forwards to its sibling overload (normalizeStorageUri(String,
            // Map) -> normalizeStorageUri(String)) would still leave a record and pass a name-only check,
            // while in fact having dropped the arguments.
            Method landed = recorder.calls.get(0);
            Assertions.assertEquals(method.getName(), landed.getName());
            Assertions.assertArrayEquals(method.getParameterTypes(), landed.getParameterTypes(),
                    method.getName() + " reached the wrapped context as a DIFFERENT overload, so its "
                            + "arguments were dropped on the way");
        }
    }

    @Test
    public void returnValueComesFromTheWrappedContext() {
        ConnectorContext wrapped = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "engine_catalog";
            }

            @Override
            public long getCatalogId() {
                return 42L;
            }
        };
        ConnectorContext forwarding = new ForwardingConnectorContext(wrapped) {
        };
        Assertions.assertEquals("engine_catalog", forwarding.getCatalogName());
        Assertions.assertEquals(42L, forwarding.getCatalogId());
    }

    @Test
    public void storageContextComesFromTheWrappedContext() {
        // Storage services reach the connector through a single forward, so a decorator can no longer lose one
        // of them however many are added. What it CAN still lose is this one getter -- and losing it is the
        // silent-downgrade failure the base class exists to prevent: the connector would get NOOP (no
        // filesystem, no credentials, no normalization) and read that as "this catalog has no storage".
        // MUTATION: deleting the getStorageContext() forward from the base class -> red here and in
        // everyContextMethodReachesTheWrappedContext, which names the missing method.
        ConnectorStorageContext engineStorage = new ConnectorStorageContext() {
        };
        ConnectorContext wrapped = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "engine_catalog";
            }

            @Override
            public long getCatalogId() {
                return 42L;
            }

            @Override
            public ConnectorStorageContext getStorageContext() {
                return engineStorage;
            }
        };
        ConnectorContext forwarding = new ForwardingConnectorContext(wrapped) {
        };
        Assertions.assertSame(engineStorage, forwarding.getStorageContext(),
                "a decorator must hand the connector the ENGINE's storage context, not the NOOP default");
    }

    @Test
    public void nullDelegateRejected() {
        Assertions.assertThrows(NullPointerException.class, () -> new ForwardingConnectorContext(null) {
        });
    }
}
