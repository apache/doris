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

#include "Adaptor.hh"
#include "OrcTest.hh"
#include "orc/Exceptions.hh"
#include "orc/Type.hh"
#include "wrap/gtest-wrapper.h"

#include "Reader.cc"
#include "TypeImpl.hh"

namespace orc {

  uint64_t checkIds(const Type* type, uint64_t next) {
    EXPECT_EQ(next, type->getColumnId()) << "Wrong id for " << type->toString();
    next += 1;
    for (uint64_t child = 0; child < type->getSubtypeCount(); ++child) {
      next = checkIds(type->getSubtype(child), next) + 1;
    }
    EXPECT_EQ(next - 1, type->getMaximumColumnId()) << "Wrong maximum id for " << type->toString();
    return type->getMaximumColumnId();
  }

  TEST(TestType, simple) {
    std::unique_ptr<Type> myType = createStructType();
    myType->addStructField("myInt", createPrimitiveType(INT));
    myType->addStructField("myString", createPrimitiveType(STRING));
    myType->addStructField("myFloat", createPrimitiveType(FLOAT));
    myType->addStructField("list", createListType(createPrimitiveType(LONG)));
    myType->addStructField("bool", createPrimitiveType(BOOLEAN));

    EXPECT_EQ(0, myType->getColumnId());
    EXPECT_EQ(6, myType->getMaximumColumnId());
    EXPECT_EQ(5, myType->getSubtypeCount());
    EXPECT_EQ(STRUCT, myType->getKind());
    EXPECT_EQ(
        "struct<myInt:int,myString:string,myFloat:float,"
        "list:array<bigint>,bool:boolean>",
        myType->toString());
    checkIds(myType.get(), 0);

    const Type* child = myType->getSubtype(0);
    EXPECT_EQ(1, child->getColumnId());
    EXPECT_EQ(1, child->getMaximumColumnId());
    EXPECT_EQ(INT, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = myType->getSubtype(1);
    EXPECT_EQ(2, child->getColumnId());
    EXPECT_EQ(2, child->getMaximumColumnId());
    EXPECT_EQ(STRING, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = myType->getSubtype(2);
    EXPECT_EQ(3, child->getColumnId());
    EXPECT_EQ(3, child->getMaximumColumnId());
    EXPECT_EQ(FLOAT, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = myType->getSubtype(3);
    EXPECT_EQ(4, child->getColumnId());
    EXPECT_EQ(5, child->getMaximumColumnId());
    EXPECT_EQ(LIST, child->getKind());
    EXPECT_EQ(1, child->getSubtypeCount());
    EXPECT_EQ("array<bigint>", child->toString());

    child = child->getSubtype(0);
    EXPECT_EQ(5, child->getColumnId());
    EXPECT_EQ(5, child->getMaximumColumnId());
    EXPECT_EQ(LONG, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = myType->getSubtype(4);
    EXPECT_EQ(6, child->getColumnId());
    EXPECT_EQ(6, child->getMaximumColumnId());
    EXPECT_EQ(BOOLEAN, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());
  }

  TEST(TestType, nested) {
    std::unique_ptr<Type> myType = createStructType();
    {
      std::unique_ptr<Type> innerStruct = createStructType();
      innerStruct->addStructField("col0", createPrimitiveType(INT));

      std::unique_ptr<Type> unionType = createUnionType();
      unionType->addUnionChild(std::move(innerStruct));
      unionType->addUnionChild(createPrimitiveType(STRING));

      myType->addStructField("myList", createListType(createMapType(createPrimitiveType(STRING),
                                                                    std::move(unionType))));
    }

    // get a pointer to the bottom type
    const Type* listType = myType->getSubtype(0);
    const Type* mapType = listType->getSubtype(0);
    const Type* unionType = mapType->getSubtype(1);
    const Type* structType = unionType->getSubtype(0);
    const Type* intType = structType->getSubtype(0);

    // calculate the id of the child to make sure that we climb correctly
    EXPECT_EQ(6, intType->getColumnId());
    EXPECT_EQ(6, intType->getMaximumColumnId());
    EXPECT_EQ("int", intType->toString());

    checkIds(myType.get(), 0);

    EXPECT_EQ(5, structType->getColumnId());
    EXPECT_EQ(6, structType->getMaximumColumnId());
    EXPECT_EQ("struct<col0:int>", structType->toString());

    EXPECT_EQ(4, unionType->getColumnId());
    EXPECT_EQ(7, unionType->getMaximumColumnId());
    EXPECT_EQ("uniontype<struct<col0:int>,string>", unionType->toString());

    EXPECT_EQ(2, mapType->getColumnId());
    EXPECT_EQ(7, mapType->getMaximumColumnId());
    EXPECT_EQ("map<string,uniontype<struct<col0:int>,string>>", mapType->toString());

    EXPECT_EQ(1, listType->getColumnId());
    EXPECT_EQ(7, listType->getMaximumColumnId());
    EXPECT_EQ("array<map<string,uniontype<struct<col0:int>,string>>>", listType->toString());

    EXPECT_EQ(0, myType->getColumnId());
    EXPECT_EQ(7, myType->getMaximumColumnId());
    EXPECT_EQ(
        "struct<myList:array<map<string,uniontype<struct<col0:int>,"
        "string>>>>",
        myType->toString());
  }

  TEST(TestType, selectedType) {
    std::unique_ptr<Type> myType = createStructType();
    myType->addStructField("col0", createPrimitiveType(BYTE));
    myType->addStructField("col1", createPrimitiveType(SHORT));
    myType->addStructField("col2", createListType(createPrimitiveType(STRING)));
    myType->addStructField("col3",
                           createMapType(createPrimitiveType(FLOAT), createPrimitiveType(DOUBLE)));
    std::unique_ptr<Type> unionType = createUnionType();
    unionType->addUnionChild(createCharType(CHAR, 100));
    unionType->addUnionChild(createCharType(VARCHAR, 200));
    myType->addStructField("col4", std::move(unionType));
    myType->addStructField("col5", createPrimitiveType(INT));
    myType->addStructField("col6", createPrimitiveType(LONG));
    myType->addStructField("col7", createDecimalType(10, 2));

    checkIds(myType.get(), 0);
    EXPECT_EQ(
        "struct<col0:tinyint,col1:smallint,col2:array<string>,"
        "col3:map<float,double>,col4:uniontype<char(100),varchar(200)>,"
        "col5:int,col6:bigint,col7:decimal(10,2)>",
        myType->toString());
    EXPECT_EQ(0, myType->getColumnId());
    EXPECT_EQ(13, myType->getMaximumColumnId());

    std::vector<bool> selected(14);
    selected[0] = true;
    selected[2] = true;
    std::unique_ptr<Type> cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col1:smallint>", cutType->toString());
    EXPECT_EQ(0, cutType->getColumnId());
    EXPECT_EQ(13, cutType->getMaximumColumnId());
    EXPECT_EQ(2, cutType->getSubtype(0)->getColumnId());

    selected.assign(14, true);
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ(
        "struct<col0:tinyint,col1:smallint,col2:array<string>,"
        "col3:map<float,double>,col4:uniontype<char(100),varchar(200)>,"
        "col5:int,col6:bigint,col7:decimal(10,2)>",
        cutType->toString());
    EXPECT_EQ(0, cutType->getColumnId());
    EXPECT_EQ(13, cutType->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[8] = true;
    selected[10] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col4:uniontype<varchar(200)>>", cutType->toString());
    EXPECT_EQ(0, cutType->getColumnId());
    EXPECT_EQ(13, cutType->getMaximumColumnId());
    EXPECT_EQ(8, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(10, cutType->getSubtype(0)->getMaximumColumnId());
    EXPECT_EQ(10, cutType->getSubtype(0)->getSubtype(0)->getColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[8] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col4:uniontype<>>", cutType->toString());

    selected.assign(14, false);
    selected[0] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<>", cutType->toString());

    selected.assign(14, false);
    selected[0] = true;
    selected[3] = true;
    selected[4] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col2:array<string>>", cutType->toString());

    selected.assign(14, false);
    selected[0] = true;
    selected[3] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col2:array<void>>", cutType->toString());
    EXPECT_EQ(3, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(4, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[5] = true;
    selected[6] = true;
    selected[7] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col3:map<float,double>>", cutType->toString());
    EXPECT_EQ(5, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(7, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[5] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col3:map<void,void>>", cutType->toString());
    EXPECT_EQ(5, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(7, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[5] = true;
    selected[6] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col3:map<float,void>>", cutType->toString());
    EXPECT_EQ(5, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(7, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[5] = true;
    selected[7] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col3:map<void,double>>", cutType->toString());
    EXPECT_EQ(5, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(7, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[1] = true;
    selected[13] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col0:tinyint,col7:decimal(10,2)>", cutType->toString());
    EXPECT_EQ(1, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(1, cutType->getSubtype(0)->getMaximumColumnId());
    EXPECT_EQ(13, cutType->getSubtype(1)->getColumnId());
    EXPECT_EQ(13, cutType->getSubtype(1)->getMaximumColumnId());
  }

  void expectLogicErrorDuringParse(std::string typeStr, const char* errMsg) {
    try {
      std::unique_ptr<Type> type = Type::buildTypeFromString(typeStr);
      FAIL() << "'" << typeStr << "'"
             << " should throw std::logic_error for invalid schema";
    } catch (std::logic_error& e) {
      EXPECT_EQ(e.what(), std::string(errMsg));
    } catch (...) {
      FAIL() << "Should only throw std::logic_error for invalid schema";
    }
  }

  TEST(TestType, buildTypeFromString) {
    std::string typeStr = "struct<a:int,b:string,c:decimal(10,2),d:varchar(5)>";
    std::unique_ptr<Type> type = Type::buildTypeFromString(typeStr);
    EXPECT_EQ(typeStr, type->toString());

    typeStr = "map<boolean,float>";
    type = Type::buildTypeFromString(typeStr);
    EXPECT_EQ(typeStr, type->toString());

    typeStr = "uniontype<bigint,binary,timestamp>";
    type = Type::buildTypeFromString(typeStr);
    EXPECT_EQ(typeStr, type->toString());

    typeStr = "struct<a:bigint,b:struct<a:binary,b:timestamp>>";
    type = Type::buildTypeFromString(typeStr);
    EXPECT_EQ(typeStr, type->toString());

    typeStr = "struct<a:bigint,b:struct<a:binary,b:timestamp with local time zone>>";
    type = Type::buildTypeFromString(typeStr);
    EXPECT_EQ(typeStr, type->toString());

    typeStr = "struct<a:bigint,b:struct<a:binary,b:timestamp>,c:map<double,tinyint>>";
    type = Type::buildTypeFromString(typeStr);
    EXPECT_EQ(typeStr, type->toString());

    typeStr = "timestamp with local time zone";
    type = Type::buildTypeFromString(typeStr);
    EXPECT_EQ(typeStr, type->toString());

    expectLogicErrorDuringParse("foobar", "Unknown type foobar");
    expectLogicErrorDuringParse("struct<col0:int>other", "Invalid type string.");
    expectLogicErrorDuringParse("array<>", "Unknown type ");
    expectLogicErrorDuringParse("array<int,string>",
                                "Array type must contain exactly one sub type.");
    expectLogicErrorDuringParse("map<int,string,double>",
                                "Map type must contain exactly two sub types.");
    expectLogicErrorDuringParse("int<>", "Invalid < after int type.");
    expectLogicErrorDuringParse("array(int)", "Missing < after array.");
    expectLogicErrorDuringParse("struct<struct<bigint>>",
                                "Invalid struct type. Field name can not contain '<'.");
    expectLogicErrorDuringParse("struct<a:bigint;b:string>", "Missing comma after field.");
  }

  TEST(TestType, quotedFieldNames) {
    std::unique_ptr<Type> type = createStructType();
    type->addStructField("foo bar", createPrimitiveType(INT));
    type->addStructField("`some`thing`", createPrimitiveType(INT));
    type->addStructField("1234567890_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
                         createPrimitiveType(INT));
    type->addStructField("'!@#$%^&*()-=_+", createPrimitiveType(INT));
    EXPECT_EQ(
        "struct<`foo bar`"
        ":int,```some``thing```:int,"
        "1234567890_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ:int,"
        "`'!@#$%^&*()-=_+`:int>",
        type->toString());

    std::string typeStr = "struct<`foo bar`:int,```quotes```:double,`abc``def````ghi`:float>";
    type = Type::buildTypeFromString(typeStr);
    EXPECT_EQ(3, type->getSubtypeCount());
    EXPECT_EQ("foo bar", type->getFieldName(0));
    EXPECT_EQ("`quotes`", type->getFieldName(1));
    EXPECT_EQ("abc`def``ghi", type->getFieldName(2));
    EXPECT_EQ(typeStr, type->toString());

    expectLogicErrorDuringParse("struct<``:int>", "Empty quoted field name.");
    expectLogicErrorDuringParse("struct<`col0:int>", "Invalid field name. Unmatched quote");
  }

  void testCorruptHelper(const proto::Type& type, const proto::Footer& footer, const char* errMsg) {
    try {
      convertType(type, footer);
      FAIL() << "Should throw ParseError for ill types";
    } catch (ParseError& e) {
      EXPECT_EQ(e.what(), std::string(errMsg));
    } catch (...) {
      FAIL() << "Should only throw ParseError for ill types";
    }
  }

  TEST(TestType, testCorruptNestType) {
    proto::Footer footer;  // not used

    proto::Type illListType;
    illListType.set_kind(proto::Type_Kind_LIST);
    testCorruptHelper(illListType, footer, "Illegal LIST type that doesn't contain one subtype");
    illListType.add_subtypes(2);
    illListType.add_subtypes(3);
    testCorruptHelper(illListType, footer, "Illegal LIST type that doesn't contain one subtype");

    proto::Type illMapType;
    illMapType.set_kind(proto::Type_Kind_MAP);
    illMapType.add_subtypes(2);
    testCorruptHelper(illMapType, footer, "Illegal MAP type that doesn't contain two subtypes");
    illMapType.add_subtypes(3);
    illMapType.add_subtypes(4);
    testCorruptHelper(illMapType, footer, "Illegal MAP type that doesn't contain two subtypes");

    proto::Type illUnionType;
    illUnionType.set_kind(proto::Type_Kind_UNION);
    testCorruptHelper(illUnionType, footer, "Illegal UNION type that doesn't contain any subtypes");

    proto::Type illStructType;
    proto::Type structType;
    illStructType.set_kind(proto::Type_Kind_STRUCT);
    structType.set_kind(proto::Type_Kind_STRUCT);
    structType.add_subtypes(0);  // construct a loop back to root
    structType.add_fieldnames("root");
    illStructType.add_subtypes(1);
    illStructType.add_fieldnames("f1");
    illStructType.add_subtypes(2);
    *(footer.add_types()) = illStructType;
    *(footer.add_types()) = structType;
    testCorruptHelper(illStructType, footer,
                      "Illegal STRUCT type that contains less fieldnames than subtypes");
  }

  void expectParseError(const proto::Footer& footer, const char* errMsg) {
    try {
      checkProtoTypes(footer);
      FAIL() << "Should throw ParseError for ill ids";
    } catch (ParseError& e) {
      EXPECT_EQ(e.what(), std::string(errMsg));
    } catch (...) {
      FAIL() << "Should only throw ParseError for ill ids";
    }
  }

  TEST(TestType, testCheckProtoTypes) {
    proto::Footer footer;
    proto::Type rootType;
    expectParseError(footer, "Footer is corrupt: no types found");

    rootType.set_kind(proto::Type_Kind_STRUCT);
    rootType.add_subtypes(1);  // add a non existent type id
    rootType.add_fieldnames("f1");
    *(footer.add_types()) = rootType;
    expectParseError(footer, "Footer is corrupt: types(1) not exists");

    footer.clear_types();
    rootType.clear_subtypes();
    rootType.clear_fieldnames();
    proto::Type structType;
    structType.set_kind(proto::Type_Kind_STRUCT);
    structType.add_subtypes(0);  // construct a loop back to root
    structType.add_fieldnames("root");
    rootType.add_subtypes(1);
    rootType.add_fieldnames("f1");
    *(footer.add_types()) = rootType;
    *(footer.add_types()) = structType;
    expectParseError(footer, "Footer is corrupt: malformed link from type 1 to 0");

    footer.clear_types();
    rootType.clear_subtypes();
    rootType.clear_fieldnames();
    proto::Type listType;
    listType.set_kind(proto::Type_Kind_LIST);
    proto::Type mapType;
    mapType.set_kind(proto::Type_Kind_MAP);
    proto::Type unionType;
    unionType.set_kind(proto::Type_Kind_UNION);
    rootType.add_fieldnames("f1");
    rootType.add_subtypes(1);           // 0 -> 1
    listType.add_subtypes(2);           // 1 -> 2
    mapType.add_subtypes(3);            // 2 -> 3
    unionType.add_subtypes(1);          // 3 -> 1
    *(footer.add_types()) = rootType;   // 0
    *(footer.add_types()) = listType;   // 1
    *(footer.add_types()) = mapType;    // 2
    *(footer.add_types()) = unionType;  // 3
    expectParseError(footer, "Footer is corrupt: malformed link from type 3 to 1");

    footer.clear_types();
    rootType.clear_subtypes();
    rootType.clear_fieldnames();
    proto::Type intType;
    intType.set_kind(proto::Type_Kind_INT);
    proto::Type strType;
    strType.set_kind(proto::Type_Kind_STRING);
    rootType.add_subtypes(2);
    rootType.add_fieldnames("f2");
    rootType.add_subtypes(1);
    rootType.add_fieldnames("f1");
    *(footer.add_types()) = rootType;
    *(footer.add_types()) = intType;
    *(footer.add_types()) = strType;
    expectParseError(footer, "Footer is corrupt: subType(0) >= subType(1) in types(0). (2 >= 1)");

    footer.clear_types();
    rootType.clear_subtypes();
    rootType.clear_fieldnames();
    rootType.set_kind(proto::Type_Kind_STRUCT);
    rootType.add_subtypes(1);
    *(footer.add_types()) = rootType;
    *(footer.add_types()) = intType;
    expectParseError(footer,
                     "Footer is corrupt: STRUCT type 0 has 1 subTypes, but has 0 fieldNames");
    // Should pass the check after adding the field name
    footer.clear_types();
    rootType.add_fieldnames("f1");
    *(footer.add_types()) = rootType;
    *(footer.add_types()) = intType;
    checkProtoTypes(footer);
  }
}  // namespace orc
