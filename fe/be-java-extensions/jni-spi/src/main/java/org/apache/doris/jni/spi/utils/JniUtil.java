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

package org.apache.doris.jni.spi.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Turns a Java exception into the text BE reports to the user.
 *
 * <p>These two methods are the whole of the SPI's exception bridge. BE calls them on any Throwable
 * that escapes a JNI entry point, and the result is what shows up in the query error message, so
 * they must not depend on anything a plugin could shadow.
 */
public class JniUtil {

    /**
     * One-line summary: the exception type and message, followed by the same for each cause. Used
     * where a query error message needs to stay readable.
     */
    public static String throwableToString(Throwable t) {
        StringWriter output = new StringWriter();
        output.write(String.format("%s: %s", t.getClass().getSimpleName(), t.getMessage()));
        // Follow the chain of exception causes and print them as well.
        Throwable cause = t;
        while ((cause = cause.getCause()) != null) {
            output.write(String.format(" | CAUSED BY: %s: %s",
                    cause.getClass().getSimpleName(), cause.getMessage()));
        }
        return output.toString();
    }

    /** Full stack trace, for the BE log. */
    public static String throwableToStackTrace(Throwable t) {
        Writer output = new StringWriter();
        t.printStackTrace(new PrintWriter(output));
        return output.toString();
    }
}
