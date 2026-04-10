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

package org.apache.doris.analysis;

import org.apache.doris.foundation.format.FormatOptions;

/**
 * Context for {@link ExprToStringValueVisitor}, carrying format options and mode information.
 */
public class StringValueContext {
    private final FormatOptions formatOptions;
    private final boolean forStreamLoad;
    private final boolean inComplexType;

    private StringValueContext(FormatOptions formatOptions, boolean forStreamLoad, boolean inComplexType) {
        this.formatOptions = formatOptions;
        this.forStreamLoad = forStreamLoad;
        this.inComplexType = inComplexType;
    }

    public static StringValueContext forQuery(FormatOptions options) {
        return new StringValueContext(options, false, false);
    }

    public static StringValueContext forStreamLoad(FormatOptions options) {
        return new StringValueContext(options, true, false);
    }

    public StringValueContext asComplexType() {
        if (inComplexType) {
            return this;
        }
        return new StringValueContext(formatOptions, forStreamLoad, true);
    }

    /**
     * Returns a context in query + complex-type mode, regardless of the current mode.
     * Used by StructLiteral's stream-load path where children are rendered in query-complex mode.
     */
    public StringValueContext asQueryComplexType() {
        return new StringValueContext(formatOptions, false, true);
    }

    public FormatOptions getFormatOptions() {
        return formatOptions;
    }

    public boolean isForStreamLoad() {
        return forStreamLoad;
    }

    public boolean isInComplexType() {
        return inComplexType;
    }
}
