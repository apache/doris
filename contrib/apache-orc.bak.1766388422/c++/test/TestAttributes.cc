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

#include "orc/OrcFile.hh"

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

#include <cmath>
#include <sstream>

namespace orc {
  const int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;  // 10M

  class TypeAttributes : public ::testing::Test {
   public:
    ~TypeAttributes() override;

   protected:
    static void SetUpTestCase() {
      memStream.reset();
    }

    static void TearDownTestCase() {}

    std::unique_ptr<Reader> createReader() {
      auto inStream =
          std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
      ReaderOptions options;
      return orc::createReader(std::move(inStream), options);
    }

    std::unique_ptr<RowReader> createRowReader(const std::unique_ptr<Reader>& reader) {
      RowReaderOptions rowReaderOpts;
      return reader->createRowReader(rowReaderOpts);
    }

    std::unique_ptr<RowReader> createRowReader(const std::unique_ptr<Reader>& reader,
                                               const std::list<uint64_t>& includeTypes) {
      RowReaderOptions rowReaderOpts;
      rowReaderOpts.includeTypes(includeTypes);
      return reader->createRowReader(rowReaderOpts);
    }

    void writeFileWithType(Type& type) {
      WriterOptions options;
      auto writer = createWriter(type, &memStream, options);
      writer->close();
    }

    const Type* getTypeByPath(const Type* root, const std::vector<uint64_t>& path) {
      const Type* ret = root;
      for (uint64_t idx : path) {
        ret = ret->getSubtype(idx);
      }
      return ret;
    }

   private:
    static MemoryOutputStream memStream;
  };

  TypeAttributes::~TypeAttributes() {}
  MemoryOutputStream TypeAttributes::memStream(DEFAULT_MEM_STREAM_SIZE);

  TEST_F(TypeAttributes, writeSimple) {
    auto intType = createPrimitiveType(INT);
    intType->setAttribute("id", "1");
    auto structType = createStructType();
    structType->addStructField("i", std::move(intType));
    writeFileWithType(*structType);

    auto reader = createReader();
    auto& root = reader->getType();
    auto i = root.getSubtype(0);
    EXPECT_EQ("1", i->getAttributeValue("id"));

    auto rowReader = createRowReader(reader, {1});
    auto& selectedRoot = rowReader->getSelectedType();
    auto selectedCol = selectedRoot.getSubtype(0);
    EXPECT_EQ("1", selectedCol->getAttributeValue("id"));
  }

  TEST_F(TypeAttributes, writeMultipleAttributes) {
    auto stringType = createPrimitiveType(STRING);
    stringType->setAttribute("foo", "xfoo");
    stringType->setAttribute("bar", "xbar");
    stringType->setAttribute("baz", "xbaz");
    // Let's also test removing an attribute.
    stringType->removeAttribute("bar");
    auto structType = createStructType();
    structType->addStructField("str", std::move(stringType));
    writeFileWithType(*structType);

    auto reader = createReader();
    auto rowReader = createRowReader(reader, {1});
    auto& root = rowReader->getSelectedType();
    auto col = root.getSubtype(0);
    auto attributeKeys = col->getAttributeKeys();

    EXPECT_EQ(2, attributeKeys.size());
    EXPECT_FALSE(col->hasAttributeKey("bar"));
    EXPECT_TRUE(col->hasAttributeKey("foo"));
    EXPECT_TRUE(col->hasAttributeKey("baz"));
    EXPECT_EQ("xfoo", col->getAttributeValue("foo"));
    EXPECT_EQ("xbaz", col->getAttributeValue("baz"));
  }

  // Tests that type and all descendants have only a single attribute,
  // and the attibute value equals to 'x' + <attribute key>.
  void testTypeHasXAttr(const Type* type) {
    auto keys = type->getAttributeKeys();
    EXPECT_EQ(1, keys.size());
    auto& key = keys.front();
    EXPECT_EQ('x' + key, type->getAttributeValue(key));
    for (uint64_t i = 0; i < type->getSubtypeCount(); ++i) {
      testTypeHasXAttr(type->getSubtype(i));
    }
  }

  TEST_F(TypeAttributes, writeAttributesForNestedTypes) {
    // Let's create struct<list:array<struct<myMap:map<int,union<long, float>>>>>
    auto intType = createPrimitiveType(INT);
    intType->setAttribute("i", "xi");
    auto longType = createPrimitiveType(FLOAT);
    longType->setAttribute("l", "xl");
    auto floatType = createPrimitiveType(FLOAT);
    floatType->setAttribute("f", "xf");
    auto unionType = createUnionType();
    unionType->setAttribute("u", "xu");
    unionType->addUnionChild(std::move(longType));
    unionType->addUnionChild(std::move(floatType));
    auto mapType = createMapType(std::move(intType), std::move(unionType));
    mapType->setAttribute("m", "xm");
    auto innerStructType = createStructType();
    innerStructType->setAttribute("is", "xis");
    innerStructType->addStructField("myMap", std::move(mapType));
    auto listType = createListType(std::move(innerStructType));
    listType->setAttribute("l", "xl");
    auto rootStructType = createStructType();
    rootStructType->addStructField("list", std::move(listType));
    writeFileWithType(*rootStructType);

    auto reader = createReader();
    auto rowReader = createRowReader(reader);
    auto& root = rowReader->getSelectedType();

    auto getVal = [this, &root](const std::vector<uint64_t>& path, const std::string& key) {
      auto t = getTypeByPath(&root, path);
      return t->getAttributeValue(key);
    };
    EXPECT_EQ("xl", getVal({0}, "l"));
    EXPECT_EQ("xis", getVal({0, 0}, "is"));
    EXPECT_EQ("xm", getVal({0, 0, 0}, "m"));
    EXPECT_EQ("xi", getVal({0, 0, 0, 0}, "i"));
    EXPECT_EQ("xu", getVal({0, 0, 0, 1}, "u"));
    EXPECT_EQ("xl", getVal({0, 0, 0, 1, 0}, "l"));
    EXPECT_EQ("xf", getVal({0, 0, 0, 1, 1}, "f"));
  }

  void collectFieldIds(const Type* t, std::vector<uint64_t>* fieldIds) {
    const std::string ICEBERG_ID = "iceberg.id";
    if (t->hasAttributeKey(ICEBERG_ID)) {
      std::string id = t->getAttributeValue(ICEBERG_ID);
      fieldIds->push_back(static_cast<uint64_t>(stoi(id)));
    } else {
      EXPECT_EQ(0, t->getColumnId());
    }
    for (uint64_t i = 0; i < t->getSubtypeCount(); ++i) {
      collectFieldIds(t->getSubtype(i), fieldIds);
    }
  }

  TEST_F(TypeAttributes, readExampleFile) {
    std::stringstream ss;
    if (const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
      ss << example_dir;
    } else {
      ss << "../../../examples";
    }
    ss << "/complextypes_iceberg.orc";
    ReaderOptions readerOpts;
    std::unique_ptr<orc::Reader> reader = orc::createReader(
        readLocalFile(ss.str().c_str(), readerOpts.getReaderMetrics()), readerOpts);
    auto rowReader = createRowReader(reader);
    auto& root = rowReader->getSelectedType();
    std::vector<uint64_t> fieldIds;
    collectFieldIds(&root, &fieldIds);
    EXPECT_EQ(29, fieldIds.size());
    sort(fieldIds.begin(), fieldIds.end());
    for (uint64_t i = 0; i < fieldIds.size(); ++i) {
      EXPECT_EQ(i + 1, fieldIds[i]);
    }
  }
}  // namespace orc
