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

package org.apache.doris.nereids.pattern.generator.javaast;

import com.google.common.base.Joiner;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/** java's class or interface's modifiers. */
public class ClassOrInterfaceModifier implements JavaAstNode {
    public final int mod;

    public ClassOrInterfaceModifier(int mod) {
        this.mod = mod;
    }

    @Override
    public String toString() {
        List<String> modifiers = new ArrayList<>(3);
        if (Modifier.isPublic(mod)) {
            modifiers.add("public");
        } else if (Modifier.isProtected(mod)) {
            modifiers.add("protected");
        } else if (Modifier.isPrivate(mod)) {
            modifiers.add("private");
        }

        if (Modifier.isStatic(mod)) {
            modifiers.add("static");
        }

        if (Modifier.isAbstract(mod)) {
            modifiers.add("abstract");
        } else if (Modifier.isFinal(mod)) {
            modifiers.add("final");
        }
        return Joiner.on(" ").join(modifiers);
    }
}
