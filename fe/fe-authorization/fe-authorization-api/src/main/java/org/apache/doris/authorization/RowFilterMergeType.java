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

package org.apache.doris.authorization;

/**
 * How the engine combines several {@link RowFilterSpec}s that apply to the same table for the same subject.
 *
 * <p>The engine owns the combination semantics (an authorization source never gets to define how its filters
 * merge with another's): restrictive filters are ANDed, permissive filters are ORed, and when both kinds are
 * present the result is {@code AND(restrictive...) AND OR(permissive...)}.</p>
 */
public enum RowFilterMergeType {
    /** The subject sees only rows matching this filter - ANDed with every other restrictive filter. */
    RESTRICTIVE,
    /** The subject additionally sees rows matching this filter - ORed with every other permissive filter. */
    PERMISSIVE
}
