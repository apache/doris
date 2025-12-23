/**
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

#ifndef ORC_BLOOMFILTER_IMPL_HH
#define ORC_BLOOMFILTER_IMPL_HH

#include "orc/BloomFilter.hh"
#include "wrap/orc-proto-wrapper.hh"

#include <cmath>
#include <sstream>
#include <vector>

namespace orc {

  /**
   * Bare metal bit set implementation. For performance reasons, this implementation does not check
   * for index bounds nor expand the bit set size if the specified index is greater than the size.
   */
  class BitSet {
   public:
    /**
     * Creates an empty BitSet
     *
     * @param numBits - number of bits used
     */
    BitSet(uint64_t numBits);

    /**
     * Creates BitSet from serialized uint64_t buffer
     *
     * @param bits - serialized uint64_t buffer of bitset
     * @param numBits - number of bits used
     */
    BitSet(const uint64_t* bits, uint64_t numBits);

    /**
     * Sets the bit at specified index.
     *
     * @param index - position
     */
    void set(uint64_t index);

    /**
     * Returns true if the bit is set in the specified index.
     *
     * @param index - position
     * @return - value at the bit position
     */
    bool get(uint64_t index);

    /**
     * Number of bits
     */
    uint64_t bitSize();

    /**
     * Combines the two BitSets using bitwise OR.
     */
    void merge(const BitSet& other);

    /**
     * Clears the bit set.
     */
    void clear();

    /**
     * Gets underlying raw data
     */
    const uint64_t* getData() const;

    /**
     * Compares two BitSets
     */
    bool operator==(const BitSet& other) const;

   private:
    std::vector<uint64_t> mData;
  };

  /**
   * BloomFilter is a probabilistic data structure for set membership check.
   * BloomFilters are highly space efficient when compared to using a HashSet.
   * Because of the probabilistic nature of bloom filter false positive (element
   * not present in bloom filter but test() says true) are possible but false
   * negatives are not possible (if element is present then test() will never
   * say false). The false positive probability is configurable (default: 5%)
   * depending on which storage requirement may increase or decrease. Lower the
   * false positive probability greater is the space requirement.
   *
   * Bloom filters are sensitive to number of elements that will be inserted in
   * the bloom filter. During the creation of bloom filter expected number of
   * entries must be specified. If the number of insertions exceed the specified
   * initial number of entries then false positive probability will increase
   * accordingly.
   *
   * Internally, this implementation of bloom filter uses Murmur3 fast
   * non-cryptographic hash algorithm. Although Murmur2 is slightly faster than
   * Murmur3 in Java, it suffers from hash collisions for specific sequence of
   * repeating bytes. Check the following link for more info
   * https://code.google.com/p/smhasher/wiki/MurmurHash2Flaw
   *
   * Note that this class is here for backwards compatibility, because it uses
   * the JVM default character set for strings. All new users should
   * BloomFilterUtf8, which always uses UTF8 for the encoding.
   */
  class BloomFilterImpl : public BloomFilter {
   public:
    /**
     * Creates an empty BloomFilter
     *
     * @param expectedEntries - number of entries it will hold
     * @param fpp - false positive probability
     */
    BloomFilterImpl(uint64_t expectedEntries, double fpp = DEFAULT_FPP);

    /**
     * Creates a BloomFilter by deserializing the proto-buf version
     *
     * caller should make sure input proto::BloomFilter is valid
     */
    BloomFilterImpl(const proto::BloomFilter& bloomFilter);

    /**
     * Adds a new element to the BloomFilter
     */
    void addBytes(const char* data, int64_t length);
    void addLong(int64_t data);
    void addDouble(double data);

    /**
     * Test if the element exists in BloomFilter
     */
    bool testBytes(const char* data, int64_t length) const override;
    bool testLong(int64_t data) const override;
    bool testDouble(double data) const override;

    uint64_t sizeInBytes() const;
    uint64_t getBitSize() const;
    int32_t getNumHashFunctions() const;

    void merge(const BloomFilterImpl& other);

    void reset();

    bool operator==(const BloomFilterImpl& other) const;

   private:
    friend struct BloomFilterUTF8Utils;
    friend class TestBloomFilter_testBloomFilterBasicOperations_Test;

    // compute k hash values from hash64 and set bits
    void addHash(int64_t hash64);

    // compute k hash values from hash64 and check bits
    bool testHash(int64_t hash64) const;

    void serialize(proto::BloomFilter& bloomFilter) const;

   private:
    static constexpr double DEFAULT_FPP = 0.05;
    uint64_t mNumBits;
    int32_t mNumHashFunctions;
    std::unique_ptr<BitSet> mBitSet;
  };

  struct BloomFilterUTF8Utils {
    // serialize BloomFilter in protobuf
    static void serialize(const BloomFilterImpl& in, proto::BloomFilter& out) {
      in.serialize(out);
    }

    // deserialize BloomFilter from protobuf
    static std::unique_ptr<BloomFilter> deserialize(const proto::Stream_Kind& streamKind,
                                                    const proto::ColumnEncoding& columnEncoding,
                                                    const proto::BloomFilter& bloomFilter);
  };

  // Thomas Wang's integer hash function
  // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
  // Put this in header file so tests can use it as well.
  inline int64_t getLongHash(int64_t key) {
    key = (~key) + (key << 21);  // key = (key << 21) - key - 1;
    key = key ^ (key >> 24);
    key = (key + (key << 3)) + (key << 8);  // key * 265
    key = key ^ (key >> 14);
    key = (key + (key << 2)) + (key << 4);  // key * 21
    key = key ^ (key >> 28);
    key = key + (key << 31);
    return key;
  }
}  // namespace orc

#endif  // ORC_BLOOMFILTER_IMPL_HH
