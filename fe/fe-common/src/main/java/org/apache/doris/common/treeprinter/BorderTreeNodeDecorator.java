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

// Author: https://github.com/davidsusu/tree-printer

package org.apache.doris.common.treeprinter;

public class BorderTreeNodeDecorator extends AbstractTreeNodeDecorator {

    public static final char[] BORDER_CHARS_ASCII = new char[]{
            '.', '-', '.', '|', '\'', '-', '`', '|'
    };

    public static final char[] BORDER_CHARS_UNICODE = new char[]{
            '┌', '─', '┐', '│', '┘', '─', '└', '│'
    };
    
    private final char topLeft;
    private final char top;
    private final char topRight;
    private final char right;
    private final char bottomRight;
    private final char bottom;
    private final char bottomLeft;
    private final char left;
    
    public BorderTreeNodeDecorator(TreeNode decoratedNode) {
        this(decoratedNode, UnicodeMode.isUnicodeDefault());
    }

    public BorderTreeNodeDecorator(TreeNode decoratedNode, boolean useUnicode) {
        this(
            decoratedNode,
            (useUnicode ? BORDER_CHARS_UNICODE : BORDER_CHARS_ASCII)[0],
            (useUnicode ? BORDER_CHARS_UNICODE : BORDER_CHARS_ASCII)[1],
            (useUnicode ? BORDER_CHARS_UNICODE : BORDER_CHARS_ASCII)[2],
            (useUnicode ? BORDER_CHARS_UNICODE : BORDER_CHARS_ASCII)[3],
            (useUnicode ? BORDER_CHARS_UNICODE : BORDER_CHARS_ASCII)[4],
            (useUnicode ? BORDER_CHARS_UNICODE : BORDER_CHARS_ASCII)[5],
            (useUnicode ? BORDER_CHARS_UNICODE : BORDER_CHARS_ASCII)[6],
            (useUnicode ? BORDER_CHARS_UNICODE : BORDER_CHARS_ASCII)[7]
        );
    }

    public BorderTreeNodeDecorator(TreeNode decoratedNode, char character) {
        this(
            decoratedNode,
            character, character, character, character,
            character, character, character, character
        );
    }

    public BorderTreeNodeDecorator(
        TreeNode decoratedNode,
        char topLeft, char top, char topRight, char right,
        char bottomRight, char bottom, char bottomLeft, char left
    ) {
        super(decoratedNode);
        this.topLeft = topLeft;
        this.top = top;
        this.topRight = topRight;
        this.right = right;
        this.bottomRight = bottomRight;
        this.bottom = bottom;
        this.bottomLeft = bottomLeft;
        this.left = left;
    }
    
    public BorderTreeNodeDecorator(
        TreeNode decoratedNode,
        boolean decorable, boolean inherit, boolean forceInherit,
        char topLeft, char top, char topRight, char right,
        char bottomRight, char bottom, char bottomLeft, char left
    ) {
        super(decoratedNode, decorable, inherit, forceInherit);
        this.topLeft = topLeft;
        this.top = top;
        this.topRight = topRight;
        this.right = right;
        this.bottomRight = bottomRight;
        this.bottom = bottom;
        this.bottomLeft = bottomLeft;
        this.left = left;
    }
    
    @Override
    public String getContent() {
        String content = decoratedNode.getContent();
        
        String[] contentLines = content.split("\n");
        int longestLineLength = 0;
        for (String line: contentLines) {
            int lineLength = line.length();
            if (lineLength > longestLineLength) {
                longestLineLength = lineLength;
            }
        }
        
        StringBuilder resultBuilder = new StringBuilder();
        
        resultBuilder.append(topLeft);
        Util.repeat(resultBuilder, top, longestLineLength);
        resultBuilder.append(topRight);
        resultBuilder.append("\n");
        for (String contentLine: contentLines) {
            resultBuilder.append(left);
            resultBuilder.append(contentLine);
            Util.repeat(resultBuilder, ' ', longestLineLength - contentLine.length());
            resultBuilder.append(right);
            resultBuilder.append("\n");
        }
        resultBuilder.append(bottomLeft);
        Util.repeat(resultBuilder, bottom, longestLineLength);
        resultBuilder.append(bottomRight);
        
        return resultBuilder.toString();
    }

    @Override
    public int[] getInsets() {
        int[] innerInsets = decoratedNode.getInsets();
        return new int[] {
            innerInsets[0] + 1,
            innerInsets[1] + 1,
            innerInsets[2] + 1,
            innerInsets[3] + 1,
        };
    }
    
    @Override
    protected TreeNode decorateChild(TreeNode childNode) {
        return new BorderTreeNodeDecorator(
            childNode,
            decorable, inherit, forceInherit,
            topLeft, top, topRight, right,
            bottomRight, bottom, bottomLeft, left
        );
    }

    public static Builder createBuilder() {
        return new Builder();
    }
    
    public static class Builder {

        private Boolean decorable = null;
        private boolean inherit = true;
        private boolean forceInherit = false;

        private char[] characters = (
            UnicodeMode.isUnicodeDefault() ?
            BORDER_CHARS_UNICODE :
            BORDER_CHARS_ASCII
        ).clone();
        
        public Builder decorable(boolean decorable) {
            this.decorable = decorable;
            return this;
        }

        public Builder decorableAuto() {
            this.decorable = null;
            return this;
        }

        public Builder inherit(boolean inherit) {
            this.inherit = inherit;
            return this;
        }

        public Builder inherit(boolean inherit, boolean forceInherit) {
            this.inherit = inherit;
            this.forceInherit = forceInherit;
            return this;
        }

        public Builder forceInherit(boolean forceInherit) {
            this.forceInherit = forceInherit;
            return this;
        }
        
        public Builder ascii() {
            this.characters = BORDER_CHARS_ASCII.clone();
            return this;
        }
        
        public Builder unicode() {
            this.characters = BORDER_CHARS_UNICODE.clone();
            return this;
        }
        
        public Builder characters(
            char topLeft, char top, char topRight, char right,
            char bottomRight, char bottom, char bottomLeft, char left
        ) {
            this.characters = new char[] {
                topLeft, top, topRight, right, bottomRight, bottom, bottomLeft, left
            };
            return this;
        }

        public Builder topLeft(char topLeft) {
            this.characters[0] = topLeft;
            return this;
        }

        public Builder top(char top) {
            this.characters[1] = top;
            return this;
        }

        public Builder topRight(char topRight) {
            this.characters[2] = topRight;
            return this;
        }

        public Builder right(char right) {
            this.characters[3] = right;
            return this;
        }

        public Builder bottomRight(char bottomRight) {
            this.characters[4] = bottomRight;
            return this;
        }

        public Builder bottom(char bottom) {
            this.characters[5] = bottom;
            return this;
        }

        public Builder bottomLeft(char bottomLeft) {
            this.characters[6] = bottomLeft;
            return this;
        }

        public Builder left(char left) {
            this.characters[7] = left;
            return this;
        }
        
        public BorderTreeNodeDecorator buildFor(TreeNode node) {
            return new BorderTreeNodeDecorator(
                node,
                decorable, inherit, forceInherit,
                characters[0], characters[1], characters[2], characters[3],
                characters[4], characters[5], characters[6], characters[7]
            );
        }
        
    }
}
