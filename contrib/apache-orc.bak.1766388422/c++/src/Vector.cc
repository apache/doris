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

#include "orc/Vector.hh"

#include "Adaptor.hh"
#include "orc/Exceptions.hh"

#include <cstdlib>
#include <iostream>
#include <sstream>

namespace orc {

  ColumnVectorBatch::ColumnVectorBatch(uint64_t cap, MemoryPool& pool)
      : capacity(cap),
        numElements(0),
        notNull(pool, cap),
        hasNulls(false),
        isEncoded(false),
        memoryPool(pool) {
    std::memset(notNull.data(), 1, capacity);
  }

  ColumnVectorBatch::~ColumnVectorBatch() {
    // PASS
  }

  void ColumnVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      capacity = cap;
      notNull.resize(cap);
    }
  }

  void ColumnVectorBatch::clear() {
    numElements = 0;
  }

  uint64_t ColumnVectorBatch::getMemoryUsage() {
    return static_cast<uint64_t>(notNull.capacity() * sizeof(char));
  }

  bool ColumnVectorBatch::hasVariableLength() {
    return false;
  }

  StringDictionary::StringDictionary(MemoryPool& pool)
      : dictionaryBlob(pool), dictionaryOffset(pool) {
    // PASS
  }

  EncodedStringVectorBatch::EncodedStringVectorBatch(uint64_t _capacity, MemoryPool& pool)
      : StringVectorBatch(_capacity, pool), dictionary(), index(pool, _capacity) {
    // PASS
  }

  EncodedStringVectorBatch::~EncodedStringVectorBatch() {
    // PASS
  }

  std::string EncodedStringVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Encoded string vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void EncodedStringVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      StringVectorBatch::resize(cap);
      index.resize(cap);
    }
  }

  StringVectorBatch::StringVectorBatch(uint64_t _capacity, MemoryPool& pool)
      : ColumnVectorBatch(_capacity, pool),
        data(pool, _capacity),
        length(pool, _capacity),
        blob(pool) {
    // PASS
  }

  StringVectorBatch::~StringVectorBatch() {
    // PASS
  }

  std::string StringVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Byte vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void StringVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
      length.resize(cap);
    }
  }

  void StringVectorBatch::clear() {
    numElements = 0;
  }

  uint64_t StringVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>(data.capacity() * sizeof(char*) +
                                 length.capacity() * sizeof(int64_t));
  }

  StructVectorBatch::StructVectorBatch(uint64_t cap, MemoryPool& pool)
      : ColumnVectorBatch(cap, pool) {
    // PASS
  }

  StructVectorBatch::~StructVectorBatch() {
    for (uint64_t i = 0; i < this->fields.size(); i++) {
      delete this->fields[i];
    }
  }

  std::string StructVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Struct vector <" << numElements << " of " << capacity << "; ";
    for (std::vector<ColumnVectorBatch*>::const_iterator ptr = fields.begin(); ptr != fields.end();
         ++ptr) {
      buffer << (*ptr)->toString() << "; ";
    }
    buffer << ">";
    return buffer.str();
  }

  void StructVectorBatch::resize(uint64_t cap) {
    ColumnVectorBatch::resize(cap);
  }

  void StructVectorBatch::clear() {
    for (size_t i = 0; i < fields.size(); i++) {
      fields[i]->clear();
    }
    numElements = 0;
  }

  uint64_t StructVectorBatch::getMemoryUsage() {
    uint64_t memory = ColumnVectorBatch::getMemoryUsage();
    for (unsigned int i = 0; i < fields.size(); i++) {
      memory += fields[i]->getMemoryUsage();
    }
    return memory;
  }

  bool StructVectorBatch::hasVariableLength() {
    for (unsigned int i = 0; i < fields.size(); i++) {
      if (fields[i]->hasVariableLength()) {
        return true;
      }
    }
    return false;
  }

  ListVectorBatch::ListVectorBatch(uint64_t cap, MemoryPool& pool)
      : ColumnVectorBatch(cap, pool), offsets(pool, cap + 1) {
    // PASS
  }

  ListVectorBatch::~ListVectorBatch() {
    // PASS
  }

  std::string ListVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "List vector <" << elements->toString() << " with " << numElements << " of "
           << capacity << ">";
    return buffer.str();
  }

  void ListVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      offsets.resize(cap + 1);
    }
  }

  void ListVectorBatch::clear() {
    numElements = 0;
    elements->clear();
  }

  uint64_t ListVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>(offsets.capacity() * sizeof(int64_t)) + elements->getMemoryUsage();
  }

  bool ListVectorBatch::hasVariableLength() {
    return true;
  }

  MapVectorBatch::MapVectorBatch(uint64_t cap, MemoryPool& pool)
      : ColumnVectorBatch(cap, pool), offsets(pool, cap + 1) {
    // PASS
  }

  MapVectorBatch::~MapVectorBatch() {
    // PASS
  }

  std::string MapVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Map vector <" << (keys ? keys->toString() : "key not selected") << ", "
           << (elements ? elements->toString() : "value not selected") << " with " << numElements
           << " of " << capacity << ">";
    return buffer.str();
  }

  void MapVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      offsets.resize(cap + 1);
    }
  }

  void MapVectorBatch::clear() {
    keys->clear();
    elements->clear();
    numElements = 0;
  }

  uint64_t MapVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>(offsets.capacity() * sizeof(int64_t)) +
           (keys ? keys->getMemoryUsage() : 0) + (elements ? elements->getMemoryUsage() : 0);
  }

  bool MapVectorBatch::hasVariableLength() {
    return true;
  }

  UnionVectorBatch::UnionVectorBatch(uint64_t cap, MemoryPool& pool)
      : ColumnVectorBatch(cap, pool), tags(pool, cap), offsets(pool, cap) {
    // PASS
  }

  UnionVectorBatch::~UnionVectorBatch() {
    for (uint64_t i = 0; i < children.size(); i++) {
      delete children[i];
    }
  }

  std::string UnionVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Union vector <";
    for (size_t i = 0; i < children.size(); ++i) {
      if (i != 0) {
        buffer << ", ";
      }
      buffer << children[i]->toString();
    }
    buffer << "; with " << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void UnionVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      tags.resize(cap);
      offsets.resize(cap);
    }
  }

  void UnionVectorBatch::clear() {
    for (size_t i = 0; i < children.size(); i++) {
      children[i]->clear();
    }
    numElements = 0;
  }

  uint64_t UnionVectorBatch::getMemoryUsage() {
    uint64_t memory = ColumnVectorBatch::getMemoryUsage() +
                      static_cast<uint64_t>(tags.capacity() * sizeof(unsigned char) +
                                            offsets.capacity() * sizeof(uint64_t));
    for (size_t i = 0; i < children.size(); ++i) {
      memory += children[i]->getMemoryUsage();
    }
    return memory;
  }

  bool UnionVectorBatch::hasVariableLength() {
    for (size_t i = 0; i < children.size(); ++i) {
      if (children[i]->hasVariableLength()) {
        return true;
      }
    }
    return false;
  }

  Decimal64VectorBatch::Decimal64VectorBatch(uint64_t cap, MemoryPool& pool)
      : ColumnVectorBatch(cap, pool),
        precision(0),
        scale(0),
        values(pool, cap),
        readScales(pool, cap) {
    // PASS
  }

  Decimal64VectorBatch::~Decimal64VectorBatch() {
    // PASS
  }

  std::string Decimal64VectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Decimal64 vector  with " << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void Decimal64VectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      values.resize(cap);
      readScales.resize(cap);
    }
  }

  void Decimal64VectorBatch::clear() {
    numElements = 0;
  }

  uint64_t Decimal64VectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>((values.capacity() + readScales.capacity()) * sizeof(int64_t));
  }

  Decimal128VectorBatch::Decimal128VectorBatch(uint64_t cap, MemoryPool& pool)
      : ColumnVectorBatch(cap, pool),
        precision(0),
        scale(0),
        values(pool, cap),
        readScales(pool, cap) {
    // PASS
  }

  Decimal128VectorBatch::~Decimal128VectorBatch() {
    // PASS
  }

  std::string Decimal128VectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Decimal128 vector  with " << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void Decimal128VectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      values.resize(cap);
      readScales.resize(cap);
    }
  }

  void Decimal128VectorBatch::clear() {
    numElements = 0;
  }

  uint64_t Decimal128VectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>(values.capacity() * sizeof(Int128) +
                                 readScales.capacity() * sizeof(int64_t));
  }

  Decimal::Decimal(const Int128& _value, int32_t _scale) : value(_value), scale(_scale) {
    // PASS
  }

  Decimal::Decimal(const std::string& str) {
    std::size_t foundPoint = str.find(".");
    // no decimal point, it is int
    if (foundPoint == std::string::npos) {
      value = Int128(str);
      scale = 0;
    } else {
      std::string copy(str);
      scale = static_cast<int32_t>(str.length() - foundPoint - 1);
      value = Int128(copy.replace(foundPoint, 1, ""));
    }
  }

  Decimal::Decimal() : value(0), scale(0) {
    // PASS
  }

  std::string Decimal::toString(bool trimTrailingZeros) const {
    return value.toDecimalString(scale, trimTrailingZeros);
  }

  TimestampVectorBatch::TimestampVectorBatch(uint64_t _capacity, MemoryPool& pool)
      : ColumnVectorBatch(_capacity, pool), data(pool, _capacity), nanoseconds(pool, _capacity) {
    // PASS
  }

  TimestampVectorBatch::~TimestampVectorBatch() {
    // PASS
  }

  std::string TimestampVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Timestamp vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void TimestampVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
      nanoseconds.resize(cap);
    }
  }

  void TimestampVectorBatch::clear() {
    numElements = 0;
  }

  uint64_t TimestampVectorBatch::getMemoryUsage() {
    return ColumnVectorBatch::getMemoryUsage() +
           static_cast<uint64_t>((data.capacity() + nanoseconds.capacity()) * sizeof(int64_t));
  }
}  // namespace orc
