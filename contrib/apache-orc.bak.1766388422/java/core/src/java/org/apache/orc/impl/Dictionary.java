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

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


/**
 * Interface to define the dictionary used for encoding value in columns
 * of specific types like string, char, varchar, etc.
 */
public interface Dictionary {
  enum IMPL {
    RBTREE,
    HASH
  }

  int INITIAL_DICTIONARY_SIZE = 4096;

  /**
   * Traverse the whole dictionary and apply the action.
   */
  void visit(Visitor visitor) throws IOException;

  void clear();

  /**
   * Given the position index, return the original string before being encoded.
   * The value of the Text in the Dictionary is copied into {@code result}.
   *
   * @param result the holder to copy the dictionary text into
   * @param position the position where the key was added
   */
  void getText(Text result, int position);

  ByteBuffer getText(int position);

  /**
   * Given the position index, write the original string, before being encoded,
   * to the OutputStream.
   *
   * @param out the output stream to which to write the data
   * @param position the position where the key was originally added
   * @return the number of byte written to the stream
   * @throws IOException if an I/O error occurs
   */
  int writeTo(OutputStream out, int position) throws IOException;

  int add(byte[] bytes, int offset, int length);

  int size();

  long getSizeInBytes();

  /**
   * The information about each node.
   */
  interface VisitorContext {
    /**
     * Get the position where the key was originally added.
     * @return the number returned by add.
     */
    int getOriginalPosition();

    /**
     * Write the bytes for the string to the given output stream.
     * @param out the stream to write to.
     * @throws IOException
     */
    void writeBytes(OutputStream out) throws IOException;

    /**
     * Get the original string.
     * @return the string
     */
    Text getText();

    /**
     * Get the number of bytes.
     * @return the string's length in bytes
     */
    int getLength();
  }

  /**
   * The interface for visitors.
   */
  interface Visitor {
    /**
     * Called once for each node of the tree in sort order.
     * @param context the information about each node
     * @throws IOException
     */
    void visit(VisitorContext context) throws IOException;
  }
}
