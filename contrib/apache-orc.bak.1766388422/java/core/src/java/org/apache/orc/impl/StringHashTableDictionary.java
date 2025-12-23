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
 * Using HashTable to represent a dictionary. The strings are stored as UTF-8 bytes
 * and an offset for each entry. It is using chaining for collision resolution.
 * <p>
 * This implementation is not thread-safe.
 */
public class StringHashTableDictionary implements Dictionary {

  // containing all keys every seen in bytes.
  private final DynamicByteArray byteArray = new DynamicByteArray();
  // containing starting offset of the key (in byte) in the byte array.
  private final DynamicIntArray keyOffsets;

  private DynamicIntArray[] hashBuckets;

  private int capacity;

  private int threshold;

  private float loadFactor;

  private static float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * Picked based on :
   * 1. default strip size (64MB),
   * 2. an assumption that record size is around 500B,
   * 3. and an assumption that there are 20% distinct keys among all keys seen within a stripe.
   * We then have the following equation:
   * 4096 * 0.75 (capacity without resize) * avgBucketSize * 5 (20% distinct) = 64 * 1024 * 1024 / 500
   * from which we deduce avgBucketSize ~8
   */
  private static final int BUCKET_SIZE = 8;

  /**
   * The maximum size of array to allocate, value being the same as {@link java.util.Hashtable},
   * given the fact that the stripe size could be increased to larger value by configuring "orc.stripe.size".
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  public StringHashTableDictionary(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public StringHashTableDictionary(int initialCapacity, float loadFactor) {
    this.capacity = initialCapacity;
    this.loadFactor = loadFactor;
    this.keyOffsets = new DynamicIntArray(initialCapacity);
    initializeHashBuckets();
    this.threshold = (int)Math.min(initialCapacity * loadFactor, MAX_ARRAY_SIZE + 1);
  }

  /**
   * Initialize the hash buckets. This will create the hash buckets if they have
   * not already been created; otherwise the existing buckets will be overwritten
   * (cleared).
   */
  private void initializeHashBuckets() {
    final DynamicIntArray[] newBuckets =
        (this.hashBuckets == null) ? new DynamicIntArray[this.capacity] : this.hashBuckets;
    for (int i = 0; i < this.capacity; i++) {
      // We don't need large bucket: If we have more than a handful of collisions,
      // then the table is too small or the function isn't good.
      newBuckets[i] = createBucket();
    }
    this.hashBuckets = newBuckets;
  }

  private DynamicIntArray createBucket() {
    return new DynamicIntArray(BUCKET_SIZE);
  }

  @Override
  public void visit(Visitor visitor)
      throws IOException {
    traverse(visitor, new VisitorContextImpl(this.byteArray, this.keyOffsets));
  }

  private void traverse(Visitor visitor, VisitorContextImpl context) throws IOException {
    for (DynamicIntArray intArray : hashBuckets) {
      for (int i = 0; i < intArray.size() ; i ++) {
        context.setPosition(intArray.get(i));
        visitor.visit(context);
      }
    }
  }

  @Override
  public void clear() {
    byteArray.clear();
    keyOffsets.clear();
    initializeHashBuckets();
  }

  @Override
  public void getText(Text result, int positionInKeyOffset) {
    DictionaryUtils.getTextInternal(result, positionInKeyOffset, this.keyOffsets, this.byteArray);
  }

  @Override
  public ByteBuffer getText(int positionInKeyOffset) {
    return DictionaryUtils.getTextInternal(positionInKeyOffset, this.keyOffsets, this.byteArray);
  }

  @Override
  public int writeTo(OutputStream out, int position) throws IOException {
    return DictionaryUtils.writeToTextInternal(out, position, this.keyOffsets, this.byteArray);
  }

  public int add(Text text) {
    return add(text.getBytes(), 0, text.getLength());
  }

  @Override
  public int add(final byte[] bytes, final int offset, final int length) {
    resizeIfNeeded();

    int index = getIndex(bytes, offset, length);
    DynamicIntArray candidateArray = hashBuckets[index];

    for (int i = 0; i < candidateArray.size(); i++) {
      final int candidateIndex = candidateArray.get(i);
      if (DictionaryUtils.equalsInternal(bytes, offset, length, candidateIndex,
          this.keyOffsets, this.byteArray)) {
        return candidateIndex;
      }
    }

    // if making it here, it means no match.
    int currIdx = keyOffsets.size();
    keyOffsets.add(byteArray.add(bytes, offset, length));
    candidateArray.add(currIdx);
    return currIdx;
  }

  private void resizeIfNeeded() {
    if (keyOffsets.size() >= threshold) {
      int oldCapacity = this.capacity;
      int newCapacity = (oldCapacity << 1) + 1;
      this.capacity = newCapacity;

      doResize(newCapacity, oldCapacity);

      this.threshold = (int)Math.min(newCapacity * loadFactor, MAX_ARRAY_SIZE + 1);
    }
  }

  @Override
  public int size() {
    return keyOffsets.size();
  }

  /**
   * Compute the hash value and find the corresponding index.
   */
  int getIndex(Text text) {
    return getIndex(text.getBytes(), 0, text.getLength());
  }

  /**
   * Compute the hash value and find the corresponding index.
   */
  int getIndex(final byte[] bytes, final int offset, final int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++) {
      hash = (31 * hash) + bytes[i];
    }
    return Math.floorMod(hash, capacity);
  }

  // Resize the hash table, re-hash all the existing keys.
  // byteArray and keyOffsetsArray don't have to be re-filled.
  private void doResize(int newCapacity, int oldCapacity) {
    DynamicIntArray[] resizedHashBuckets = new DynamicIntArray[newCapacity];
    for (int i = 0; i < newCapacity; i++) {
      resizedHashBuckets[i] = createBucket();
    }

    for (int i = 0; i < oldCapacity; i++) {
      DynamicIntArray oldBucket = hashBuckets[i];
      for (int j = 0; j < oldBucket.size(); j++) {
        final int offset = oldBucket.get(j);
        ByteBuffer text = getText(offset);
        resizedHashBuckets[getIndex(text.array(),
                text.position(), text.remaining())].add(oldBucket.get(j));
      }
    }

    hashBuckets = resizedHashBuckets;
  }

  @Override
  public long getSizeInBytes() {
    long bucketTotalSize = 0L;
    for (DynamicIntArray dynamicIntArray : hashBuckets) {
      bucketTotalSize += dynamicIntArray.size();
    }

    return byteArray.getSizeInBytes() + keyOffsets.getSizeInBytes() + bucketTotalSize ;
  }

}
