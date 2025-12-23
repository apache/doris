/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.hadoop.io.IntWritable;
import org.apache.orc.StringDictTestingUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test the red-black tree with string keys.
 */
public class TestStringRedBlackTree {

  /**
   * Checks the red-black tree rules to make sure that we have correctly built
   * a valid tree.
   *
   * Properties:
   *   1. Red nodes must have black children
   *   2. Each node must have the same black height on both sides.
   *
   * @param node The id of the root of the subtree to check for the red-black
   *        tree properties.
   * @return The black-height of the subtree.
   */
  private int checkSubtree(RedBlackTree tree, int node, IntWritable count
                          ) throws IOException {
    if (node == RedBlackTree.NULL) {
      return 1;
    }
    count.set(count.get() + 1);
    boolean is_red = tree.isRed(node);
    int left = tree.getLeft(node);
    int right = tree.getRight(node);
    if (is_red) {
      if (tree.isRed(left)) {
        printTree(tree, "", tree.root);
        throw new IllegalStateException("Left node of " + node + " is " + left +
          " and both are red.");
      }
      if (tree.isRed(right)) {
        printTree(tree, "", tree.root);
        throw new IllegalStateException("Right node of " + node + " is " +
          right + " and both are red.");
      }
    }
    int left_depth = checkSubtree(tree, left, count);
    int right_depth = checkSubtree(tree, right, count);
    if (left_depth != right_depth) {
      printTree(tree, "", tree.root);
      throw new IllegalStateException("Lopsided tree at node " + node +
        " with depths " + left_depth + " and " + right_depth);
    }
    if (is_red) {
      return left_depth;
    } else {
      return left_depth + 1;
    }
  }

  /**
   * Checks the validity of the entire tree. Also ensures that the number of
   * nodes visited is the same as the size of the set.
   */
  void checkTree(RedBlackTree tree) throws IOException {
    IntWritable count = new IntWritable(0);
    if (tree.isRed(tree.root)) {
      printTree(tree, "", tree.root);
      throw new IllegalStateException("root is red");
    }
    checkSubtree(tree, tree.root, count);
    if (count.get() != tree.size) {
      printTree(tree, "", tree.root);
      throw new IllegalStateException("Broken tree! visited= " + count.get() +
        " size=" + tree.size);
    }
  }

  void printTree(RedBlackTree tree, String indent, int node
                ) throws IOException {
    if (node == RedBlackTree.NULL) {
      System.err.println(indent + "NULL");
    } else {
      System.err.println(indent + "Node " + node + " color " +
        (tree.isRed(node) ? "red" : "black"));
      printTree(tree, indent + "  ", tree.getLeft(node));
      printTree(tree, indent + "  ", tree.getRight(node));
    }
  }

  StringRedBlackTree buildTree(String... params) throws IOException {
    StringRedBlackTree result = new StringRedBlackTree(1000);
    for(String word: params) {
      result.add(word);
      checkTree(result);
    }
    return result;
  }

  @Test
  public void test1() throws Exception {
    StringRedBlackTree tree = new StringRedBlackTree(5);
    assertEquals(0, tree.getSizeInBytes());
    checkTree(tree);
    assertEquals(0, tree.add("owen"));
    checkTree(tree);
    assertEquals(1, tree.add("ashutosh"));
    checkTree(tree);
    assertEquals(0, tree.add("owen"));
    checkTree(tree);
    assertEquals(2, tree.add("alan"));
    checkTree(tree);
    assertEquals(2, tree.add("alan"));
    checkTree(tree);
    assertEquals(1, tree.add("ashutosh"));
    checkTree(tree);
    assertEquals(3, tree.add("greg"));
    checkTree(tree);
    assertEquals(4, tree.add("eric"));
    checkTree(tree);
    assertEquals(5, tree.add("arun"));
    checkTree(tree);
    assertEquals(6, tree.size());
    checkTree(tree);
    assertEquals(6, tree.add("eric14"));
    checkTree(tree);
    assertEquals(7, tree.add("o"));
    checkTree(tree);
    assertEquals(8, tree.add("ziggy"));
    checkTree(tree);
    assertEquals(9, tree.add("z"));
    checkTree(tree);
    StringDictTestingUtils.checkContents(tree, new int[]{2,5,1,4,6,3,7,0,9,8},
      "alan", "arun", "ashutosh", "eric", "eric14", "greg",
      "o", "owen", "z", "ziggy");
    assertEquals(32888, tree.getSizeInBytes());
    // check that adding greg again bumps the count
    assertEquals(3, tree.add("greg"));
    assertEquals(41, tree.getCharacterSize());
    // add some more strings to test the different branches of the
    // rebalancing
    assertEquals(10, tree.add("zak"));
    checkTree(tree);
    assertEquals(11, tree.add("eric1"));
    checkTree(tree);
    assertEquals(12, tree.add("ash"));
    checkTree(tree);
    assertEquals(13, tree.add("harry"));
    checkTree(tree);
    assertEquals(14, tree.add("john"));
    checkTree(tree);
    tree.clear();
    checkTree(tree);
    assertEquals(0, tree.getSizeInBytes());
    assertEquals(0, tree.getCharacterSize());
  }

  @Test
  public void test2() throws Exception {
    StringRedBlackTree tree =
      buildTree("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
        "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
    assertEquals(26, tree.size());
    StringDictTestingUtils.checkContents(tree, new int[]{0,1,2, 3,4,5, 6,7,8, 9,10,11, 12,13,14,
      15,16,17, 18,19,20, 21,22,23, 24,25},
      "a", "b", "c", "d", "e", "f", "g", "h", "i", "j","k", "l", "m", "n", "o",
      "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
  }

  @Test
  public void test3() throws Exception {
    StringRedBlackTree tree =
      buildTree("z", "y", "x", "w", "v", "u", "t", "s", "r", "q", "p", "o", "n",
        "m", "l", "k", "j", "i", "h", "g", "f", "e", "d", "c", "b", "a");
    assertEquals(26, tree.size());
    StringDictTestingUtils.checkContents(tree, new int[]{25,24,23, 22,21,20, 19,18,17, 16,15,14,
      13,12,11, 10,9,8, 7,6,5, 4,3,2, 1,0},
      "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
      "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z");
  }
}
