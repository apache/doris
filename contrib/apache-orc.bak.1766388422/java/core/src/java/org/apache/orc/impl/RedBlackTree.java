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

/**
 * A memory efficient red-black tree that does not allocate any objects per
 * an element. This class is abstract and assumes that the child class
 * handles the key and comparisons with the key.
 */
abstract class RedBlackTree {
  public static final int NULL = -1;

  // Various values controlling the offset of the data within the array.
  private static final int LEFT_OFFSET = 0;
  private static final int RIGHT_OFFSET = 1;
  private static final int ELEMENT_SIZE = 2;

  protected int size = 0;
  private final DynamicIntArray data;
  protected int root = NULL;
  protected int lastAdd = 0;
  private boolean wasAdd = false;

  /**
   * Create a set with the given initial capacity.
   */
  RedBlackTree(int initialCapacity) {
    data = new DynamicIntArray(initialCapacity * ELEMENT_SIZE);
  }

  /**
   * Insert a new node into the data array, growing the array as necessary.
   *
   * @return Returns the position of the new node.
   */
  private int insert(int left, int right, boolean isRed) {
    int position = size;
    size += 1;
    setLeft(position, left, isRed);
    setRight(position, right);
    return position;
  }

  /**
   * Compare the value at the given position to the new value.
   * @return 0 if the values are the same, -1 if the new value is smaller and
   *         1 if the new value is larger.
   */
  protected abstract int compareValue(int position);

  /**
   * Is the given node red as opposed to black? To prevent having an extra word
   * in the data array, we just the low bit on the left child index.
   */
  protected boolean isRed(int position) {
    return position != NULL &&
        (data.get(position * ELEMENT_SIZE + LEFT_OFFSET) & 1) == 1;
  }

  /**
   * Set the red bit true or false.
   */
  private void setRed(int position, boolean isRed) {
    int offset = position * ELEMENT_SIZE + LEFT_OFFSET;
    if (isRed) {
      data.set(offset, data.get(offset) | 1);
    } else {
      data.set(offset, data.get(offset) & ~1);
    }
  }

  /**
   * Get the left field of the given position.
   */
  protected int getLeft(int position) {
    return data.get(position * ELEMENT_SIZE + LEFT_OFFSET) >> 1;
  }

  /**
   * Get the right field of the given position.
   */
  protected int getRight(int position) {
    return data.get(position * ELEMENT_SIZE + RIGHT_OFFSET);
  }

  /**
   * Set the left field of the given position.
   * Note that we are storing the node color in the low bit of the left pointer.
   */
  private void setLeft(int position, int left) {
    int offset = position * ELEMENT_SIZE + LEFT_OFFSET;
    data.set(offset, (left << 1) | (data.get(offset) & 1));
  }

  /**
   * Set the left field of the given position.
   * Note that we are storing the node color in the low bit of the left pointer.
   */
  private void setLeft(int position, int left, boolean isRed) {
    int offset = position * ELEMENT_SIZE + LEFT_OFFSET;
    data.set(offset, (left << 1) | (isRed ? 1 : 0));
  }

  /**
   * Set the right field of the given position.
   */
  private void setRight(int position, int right) {
    data.set(position * ELEMENT_SIZE + RIGHT_OFFSET, right);
  }

  /**
   * Insert or find a given key in the tree and rebalance the tree correctly.
   * Rebalancing restores the red-black aspect of the tree to maintain the
   * invariants:
   *   1. If a node is red, both of its children are black.
   *   2. Each child of a node has the same black height (the number of black
   *      nodes between it and the leaves of the tree).
   *
   * Inserted nodes are at the leaves and are red, therefore there is at most a
   * violation of rule 1 at the node we just put in. Instead of always keeping
   * the parents, this routine passing down the context.
   *
   * The fix is broken down into 6 cases (1.{1,2,3} and 2.{1,2,3} that are
   * left-right mirror images of each other). See Algorithms by Cormen,
   * Leiserson, and Rivest for the explanation of the subcases.
   *
   * @param node The node that we are fixing right now.
   * @param fromLeft Did we come down from the left?
   * @param parent Nodes' parent
   * @param grandparent Parent's parent
   * @param greatGrandparent Grandparent's parent
   * @return Does parent also need to be checked and/or fixed?
   */
  private boolean add(int node, boolean fromLeft, int parent,
                      int grandparent, int greatGrandparent) {
    if (node == NULL) {
      if (root == NULL) {
        lastAdd = insert(NULL, NULL, false);
        root = lastAdd;
        wasAdd = true;
        return false;
      } else {
        lastAdd = insert(NULL, NULL, true);
        node = lastAdd;
        wasAdd = true;
        // connect the new node into the tree
        if (fromLeft) {
          setLeft(parent, node);
        } else {
          setRight(parent, node);
        }
      }
    } else {
      int compare = compareValue(node);
      boolean keepGoing;

      // Recurse down to find where the node needs to be added
      if (compare < 0) {
        keepGoing = add(getLeft(node), true, node, parent, grandparent);
      } else if (compare > 0) {
        keepGoing = add(getRight(node), false, node, parent, grandparent);
      } else {
        lastAdd = node;
        wasAdd = false;
        return false;
      }

      // we don't need to fix the root (because it is always set to black)
      if (node == root || !keepGoing) {
        return false;
      }
    }


    // Do we need to fix this node? Only if there are two reds right under each
    // other.
    if (isRed(node) && isRed(parent)) {
      if (parent == getLeft(grandparent)) {
        int uncle = getRight(grandparent);
        if (isRed(uncle)) {
          // case 1.1
          setRed(parent, false);
          setRed(uncle, false);
          setRed(grandparent, true);
          return true;
        } else {
          if (node == getRight(parent)) {
            // case 1.2
            // swap node and parent
            int tmp = node;
            node = parent;
            parent = tmp;
            // left-rotate on node
            setLeft(grandparent, parent);
            setRight(node, getLeft(parent));
            setLeft(parent, node);
          }

          // case 1.2 and 1.3
          setRed(parent, false);
          setRed(grandparent, true);

          // right-rotate on grandparent
          if (greatGrandparent == NULL) {
            root = parent;
          } else if (getLeft(greatGrandparent) == grandparent) {
            setLeft(greatGrandparent, parent);
          } else {
            setRight(greatGrandparent, parent);
          }
          setLeft(grandparent, getRight(parent));
          setRight(parent, grandparent);
          return false;
        }
      } else {
        int uncle = getLeft(grandparent);
        if (isRed(uncle)) {
          // case 2.1
          setRed(parent, false);
          setRed(uncle, false);
          setRed(grandparent, true);
          return true;
        } else {
          if (node == getLeft(parent)) {
            // case 2.2
            // swap node and parent
            int tmp = node;
            node = parent;
            parent = tmp;
            // right-rotate on node
            setRight(grandparent, parent);
            setLeft(node, getRight(parent));
            setRight(parent, node);
          }
          // case 2.2 and 2.3
          setRed(parent, false);
          setRed(grandparent, true);
          // left-rotate on grandparent
          if (greatGrandparent == NULL) {
            root = parent;
          } else if (getRight(greatGrandparent) == grandparent) {
            setRight(greatGrandparent, parent);
          } else {
            setLeft(greatGrandparent, parent);
          }
          setRight(grandparent, getLeft(parent));
          setLeft(parent, grandparent);
          return false;
        }
      }
    } else {
      return true;
    }
  }

  /**
   * Add the new key to the tree.
   * @return true if the element is a new one.
   */
  protected boolean add() {
    add(root, false, NULL, NULL, NULL);
    if (wasAdd) {
      setRed(root, false);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Get the number of elements in the set.
   */
  public int size() {
    return size;
  }

  /**
   * Reset the table to empty.
   */
  public void clear() {
    root = NULL;
    size = 0;
    data.clear();
  }

  /**
   * Get the buffer size in bytes.
   */
  public long getSizeInBytes() {
    return data.getSizeInBytes();
  }
}

