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

package org.apache.doris.jni.spi;

/**
 * Creates the executor object backing one kind of Java function: scalar UDF, aggregate UDAF or
 * table-valued UDTF, distinguished by {@link #getName()}.
 *
 * <h2>Why the parameters are a byte array and the result is Object</h2>
 *
 * <p>Function parameters reach BE as a serialized thrift struct. Thrift is deliberately absent from
 * the SPI — it is the one payload that crosses the boundary, and admitting it would drag
 * libthrift plus the generated classes onto the shared layer, which is the exact coupling this
 * architecture removes. So the SPI carries the opaque bytes and the plugin, which owns its own
 * thrift copy, deserializes them.
 *
 * <p>The returned executor is {@code Object} for the mirror-image reason: its class lives in the
 * plugin and the three function kinds do not share a method shape. BE resolves {@code evaluate} and
 * {@code close} on the concrete class it gets back. Promoting a common executor base class into the
 * SPI is worth revisiting once the three kinds are unified, and only then.
 */
public interface UdfExecutorFactory {

    /** Key BE addresses this factory by, unique within the plugin. */
    String getName();

    /**
     * @param thriftParams serialized function parameters, deserialized by the plugin
     * @return the executor instance; never null
     */
    Object create(byte[] thriftParams) throws Exception;

    /**
     * Release whatever was cached for one user function, because it has been dropped.
     *
     * <p>Compiling a user function means building a classloader over the user's jar and keeping it
     * for the life of the process - there is no safe time-based eviction, since closing a
     * classloader another query still holds fails that query with a NoClassDefFoundError. DROP
     * FUNCTION is the one moment when discarding it is correct, so BE forwards it here.
     *
     * <p>Called for every loaded plugin, not only the one that ran the function: BE drops a
     * function without knowing which plugin executed it. A function this factory never cached is
     * therefore normal, and the default does nothing.
     *
     * @param functionId        FE's id for the function, the same one it puts on every request that
     *                          executes it. This is the identity to cache by: it is unique within
     *                          the cluster, so two same-named functions in different databases are
     *                          two entries, and a function dropped and re-created is a third -
     *                          none of which is true of the signature. Not positive only if FE sent
     *                          no id at all.
     * @param functionSignature {@code name(argTypes)} as FE renders it, for logs and for the
     *                          id-less case. Not an identity: it carries no database, and FE spells
     *                          a variadic signature differently here than on the requests that
     *                          execute the function.
     */
    default void invalidate(long functionId, String functionSignature) {
    }
}
