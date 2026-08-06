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

package org.apache.doris.nereids.parser;

/**
 * Optional fast-path SPI for the parser's "current Origin" stack. A Thread
 * subclass that runs the parser hot-path may implement this so {@link ParserUtils}
 * can store and load the Origin in a plain field instead of paying a ThreadLocal
 * lookup on every {@code withOrigin} call. Threads that don't implement this
 * fall back to a ThreadLocal — correctness is identical either way.
 *
 * Implementations must only be called by the owning thread; no synchronization
 * is required or expected.
 */
public interface OriginAware {
    Origin getOrigin();

    void setOrigin(Origin origin);
}
