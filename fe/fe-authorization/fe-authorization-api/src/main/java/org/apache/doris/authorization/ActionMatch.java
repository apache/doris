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
 * How many of an {@link AccessRequirement}'s actions the subject must hold.
 *
 * <p>Both are in daily use and they are not interchangeable: reading a table requires
 * {@link #ANY} of several privileges, whereas granting a privilege to someone else requires
 * {@link #ALL} of "the privilege being granted" and "the right to grant".</p>
 */
public enum ActionMatch {
    /** The subject must hold at least one of the actions. */
    ANY,
    /** The subject must hold every one of the actions. */
    ALL
}
