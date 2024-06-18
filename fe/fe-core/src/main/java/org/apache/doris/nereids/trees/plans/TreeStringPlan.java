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

package org.apache.doris.nereids.trees.plans;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.LineIterator;

import java.io.StringReader;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

/** TreeStringPlan */
public interface TreeStringPlan {
    String getChildrenTreeString();

    /** parseTreeStringNode */
    default Optional<TreeStringNode> parseTreeStringNode() {
        String treeString = getChildrenTreeString();
        LineIterator lineIt = new LineIterator(new StringReader(treeString));

        if (!lineIt.hasNext()) {
            return Optional.empty();
        }

        Stack<ParseTreeStringNodeContext> parseStack = new Stack<>();
        parseStack.push(new ParseTreeStringNodeContext(lineIt.next()));

        while (lineIt.hasNext()) {
            String line = lineIt.next();
            int level = getLevel(line);
            while (parseStack.size() >= level) {
                ParseTreeStringNodeContext child = parseStack.pop();
                parseStack.peek().children.add(
                        new TreeStringNode(child.currentString, ImmutableList.copyOf(child.children))
                );
            }
            parseStack.push(new ParseTreeStringNodeContext(line.substring((level - 1) * 3)));
        }

        while (parseStack.size() > 1) {
            ParseTreeStringNodeContext child = parseStack.pop();
            parseStack.peek().children.add(
                    new TreeStringNode(child.currentString, ImmutableList.copyOf(child.children))
            );
        }

        ParseTreeStringNodeContext top = parseStack.pop();
        return Optional.of(new TreeStringNode(top.currentString, ImmutableList.copyOf(top.children)));
    }

    /** TreeStringNode */
    class TreeStringNode {
        /** currentString */
        public final String currentString;
        /** children */
        public final List<TreeStringNode> children;

        public TreeStringNode(String currentString, List<TreeStringNode> children) {
            this.currentString = currentString;
            this.children = children;
        }

        @Override
        public String toString() {
            return currentString.trim();
        }
    }

    /** ParseTreeStringNodeContext */
    class ParseTreeStringNodeContext {
        /** currentString */
        public final String currentString;
        /** children */
        public final List<TreeStringNode> children;

        public ParseTreeStringNodeContext(String currentString) {
            this.currentString = currentString;
            this.children = Lists.newArrayList();
        }
    }

    /** getLevel */
    static int getLevel(String currentString) {
        int prefix = 0;
        for (int i = 0; i < currentString.length(); i++) {
            char c = currentString.charAt(i);
            if (c == ' ' || c == '|' || c == '-' || c == '+') {
                prefix++;
            } else {
                break;
            }
        }
        return prefix / 3 + 1;
    }
}
