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

public class Main {

    public static void main(String[] args) {
        TestNode rootNode = new TestNode("root");
        TestNode subNode1 = new TestNode("SUB asdf\nSSS fdsa\nxxx yyy");
        TestNode subNode2 = new TestNode("lorem ipsum");
        TestNode subNode3 = new TestNode("ggggg");
        TestNode subSubNode11 = new TestNode("AAA");
        TestNode subSubNode12 = new TestNode("BBB");
        TestNode subSubNode21 = new TestNode("CCC");
        TestNode subSubNode22 = new TestNode("DDD");
        TestNode subSubNode23 = new TestNode("EEE");
        TestNode subSubNode24 = new TestNode("FFF");
        TestNode subSubNode31 = new TestNode("GGG");
        TestNode subSubSubNode231 = new TestNode("(eee)");
        TestNode subSubSubNode232 = new TestNode("(eee2)");
        TestNode subSubSubNode311 = new TestNode("(ggg)");

        rootNode.addChild(subNode1);
        rootNode.addChild(subNode2);
        rootNode.addChild(subNode3);
        subNode1.addChild(subSubNode11);
        subNode1.addChild(subSubNode12);
        subNode2.addChild(subSubNode21);
        subNode2.addChild(subSubNode22);
        subNode2.addChild(subSubNode23);
        subNode2.addChild(subSubNode24);
        subNode2.addChild(null);
        subNode3.addChild(subSubNode31);
        subSubNode23.addChild(subSubSubNode231);
        subSubNode23.addChild(subSubSubNode232);
        subSubNode31.addChild(subSubSubNode311);
        subSubNode31.addChild(null);

        (new ListingTreePrinter()).print(rootNode);

        System.out.println();
        System.out.println("=====================");
        System.out.println();

        ListingTreePrinter.createBuilder().ascii().liningSpace("...").build().print(rootNode);

        System.out.println();
        System.out.println("=====================");
        System.out.println();

        (new ListingTreePrinter(false, true)).print(new PadTreeNodeDecorator(rootNode, true, true, true, 0, 0, 1, 0));

        System.out.println();
        System.out.println("=====================");
        System.out.println();
        
        (new ListingTreePrinter()).print(new BorderTreeNodeDecorator(new PadTreeNodeDecorator(rootNode, 1, 2, 1, 2)));

        System.out.println();
        System.out.println("=====================");
        System.out.println();

        (new TraditionalTreePrinter()).print(new BorderTreeNodeDecorator(rootNode));
    }
    
    private static class TestNode extends SimpleTreeNode {

        TestNode(String content) {
            super(content);
        }

        @Override
        public boolean isDecorable() {
            return (content.isEmpty() || content.charAt(0) != '(');
        }
        
    }
    
}
