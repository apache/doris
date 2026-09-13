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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a default SPI method that a connector is nevertheless expected to override.
 *
 * <p>Every method of {@link ConnectorMetadata} and its sub-interfaces has a default body, so the compiler
 * forces nothing on an implementor: an empty implementation compiles and loads, and then answers "no tables"
 * / "table not found" to users instead of failing. This annotation is the machine-readable half of the fix —
 * it says WHICH methods make up the minimum implementation set; the interface's class javadoc says WHY, and
 * what the visible symptom of skipping one is.</p>
 *
 * <p>Nothing in production reads this annotation: there is no registration-time validation and no build gate
 * (a check that has to understand which capability a connector declared cannot be written as a text match).
 * The retention is {@code RUNTIME} only so that a unit test can reflect over it and pin the set, which makes
 * promoting a method to "must implement" a deliberate, reviewed change rather than a silent one.</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConnectorMustImplement {

    /**
     * The precondition that makes this method mandatory. Empty means unconditional — every connector must
     * override it. Otherwise a short phrase naming the capability or situation that triggers the obligation.
     */
    String when() default "";
}
