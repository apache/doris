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

package org.apache.doris.qe;

import org.apache.doris.common.VariableAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Variable attribute definitions for session and global variables.
 */
public class VarAttrDef {
    // variable have this flag means that every session have a copy of this variable,
    // and can modify its own variable.
    public static final int SESSION = 1;
    // Variables with this flag have only one instance in one process.
    public static final int GLOBAL = 2;
    // Variables with this flag only exist in each session.
    public static final int SESSION_ONLY = 4;
    // Variables with this flag can only be read.
    public static final int READ_ONLY = 8;
    // Variables with this flag can not be seen with `SHOW VARIABLES` statement.
    public static final int INVISIBLE = 16;

    @Retention(RetentionPolicy.RUNTIME)
    public @interface VarAttr {
        // Name in show variables and set statement;
        String name();

        String[] alias() default {};

        int flag() default 0;

        // TODO(zhaochun): min and max is not used.
        String minValue() default "0";

        String maxValue() default "0";

        // the function name that check the VarAttr before setting it to sessionVariable
        // only support check function: 0 argument and 0 return value, if an error occurs, throw an exception.
        // the checker function should be: public void checker(String value), value is the input string.
        String checker() default "";

        // could specify the setter method for a variable, not depend on reflect mechanism
        String setter() default "";

        // Set to true if the variables need to be forwarded along with forward statement.
        boolean needForward() default false;

        // Set to true if this variable is fuzzy
        boolean fuzzy() default false;

        VariableAnnotation varType() default VariableAnnotation.NONE;

        // description for this config item.
        // There should be 2 elements in the array.
        // The first element is the description in Chinese.
        // The second element is the description in English.
        String[] description() default {"待补充", "TODO"};

        // Enum options for this config item, if it has.
        String[] options() default {};

        String convertBoolToLongMethod() default "";
        // If the variable affects the outcome, set it to true.
        // If this value is true, it will ignore needForward and enforce forwarding.
        boolean affectQueryResultInPlan() default false;

        boolean affectQueryResultInExecution() default false;
    }
}
