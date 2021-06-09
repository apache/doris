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
#pragma once

#include <rapidjson/document.h>

#include <string>

#include "gutil/ref_counted.h"

namespace doris {

// A wrapper around rapidjson Value objects, to simplify usage.
// Intended solely for building json objects, not writing/parsing.
//
// Simplifies code like this:
//
//   rapidjson::Document d;
//   rapidjson::Value v;
//   v.SetObject();
//   rapidjson::Value list;
//   list.SetArray();
//   v.AddMember("list", list, d.GetAllocator());
//   v["list"].PushBack(rapidjson::Value().SetString("element"), d.GetAllocator());
//
// To this:
//
//   EasyJson ej;
//   ej["list"][0] = "element";
//
// Client code should build objects as demonstrated above,
// then call EasyJson::value() to obtain a reference to the
// built rapidjson Value.
class EasyJson {
public:
    // Used for initializing EasyJson's with complex types.
    // For example:
    //
    //  EasyJson array;
    //  EasyJson nested = array.PushBack(EasyJson::kObject);
    //  nested["attr"] = "val";
    //  // array = [ { "attr": "val" } ]
    enum ComplexTypeInitializer { kObject, kArray };

    EasyJson();
    // Initializes the EasyJson object with the given type.
    explicit EasyJson(ComplexTypeInitializer type);
    ~EasyJson() = default;

    // Returns the child EasyJson associated with key.
    //
    // Note: this method can mutate the EasyJson object
    // as follows:
    //
    // If this EasyJson's underlying Value is not an object
    // (i.e. !this->value().IsObject()), then its Value is
    // coerced to an object, overwriting the old Value.
    // If the given key does not exist, a Null-valued
    // EasyJson associated with key is created.
    EasyJson Get(const std::string& key);

    // Returns the child EasyJson at index.
    //
    // Note: this method can mutate the EasyJson object
    // as follows:
    //
    // If this EasyJson's underlying Value is not an array
    // (i.e. !this->value().IsArray()), then its Value is
    // coerced to an array, overwriting the old Value.
    // If index >= this->value().Size(), then the underlying
    // array's size is increased to index + 1 (new indices
    // are filled with Null values).
    EasyJson Get(int index);

    // Same as Get(key).
    EasyJson operator[](const std::string& key);
    // Same as Get(index).
    EasyJson operator[](int index);

    // Sets the underlying Value equal to val.
    // Returns a reference to the object itself.
    //
    // 'val' can be a bool, int32_t, int64_t, double,
    // char*, string, or ComplexTypeInitializer.
    EasyJson& operator=(const std::string& val);
    template <typename T>
    EasyJson& operator=(T val);

    // Sets the underlying Value to an object.
    // Returns a reference to the object itself.
    //
    // i.e. after calling SetObject(),
    // value().IsObject() == true
    EasyJson& SetObject();
    // Sets the underlying Value to an array.
    // Returns a reference to the object itself.
    //
    // i.e. after calling SetArray(),
    // value().IsArray() == true
    EasyJson& SetArray();

    // Associates val with key.
    // Returns the child object.
    //
    // If this EasyJson's underlying Value is not an object
    // (i.e. !this->value().IsObject()), then its Value is
    // coerced to an object, overwriting the old Value.
    // If the given key does not exist, a new child entry
    // is created with the given value.
    EasyJson Set(const std::string& key, const std::string& val);
    template <typename T>
    EasyJson Set(const std::string& key, T val);

    // Stores val at index.
    // Returns the child object.
    //
    // If this EasyJson's underlying Value is not an array
    // (i.e. !this->value().IsArray()), then its Value is
    // coerced to an array, overwriting the old Value.
    // If index >= this->value().Size(), then the underlying
    // array's size is increased to index + 1 (new indices
    // are filled with Null values).
    EasyJson Set(int index, const std::string& val);
    template <typename T>
    EasyJson Set(int index, T val);

    // Appends val to the underlying array.
    // Returns a reference to the new child object.
    //
    // If this EasyJson's underlying Value is not an array
    // (i.e. !this->value().IsArray()), then its Value is
    // coerced to an array, overwriting the old Value.
    EasyJson PushBack(const std::string& val);
    template <typename T>
    EasyJson PushBack(T val);

    // Returns a reference to the underlying Value.
    rapidjson::Value& value() const { return *value_; }

    // Returns a string representation of the underlying json.
    std::string ToString() const;

private:
    // One instance of EasyJsonAllocator is shared among a root
    // EasyJson object and all of its descendants. The allocator
    // owns the underlying rapidjson Value, and a rapidjson
    // allocator (via a rapidjson::Document).
    class EasyJsonAllocator : public RefCounted<EasyJsonAllocator> {
    public:
        rapidjson::Value& value() { return value_; }
        rapidjson::Document::AllocatorType& allocator() { return value_.GetAllocator(); }

    private:
        friend class RefCounted<EasyJsonAllocator>;
        ~EasyJsonAllocator() = default;

        // The underlying rapidjson::Value object (Document is
        // a subclass of Value that has its own allocator).
        rapidjson::Document value_;
    };

    // Used to instantiate descendant objects.
    EasyJson(rapidjson::Value* value, scoped_refptr<EasyJsonAllocator> alloc);

    // One allocator is shared among an EasyJson object and
    // all of its descendants.
    scoped_refptr<EasyJsonAllocator> alloc_;

    // A pointer to the underlying Value in the object
    // tree owned by alloc_.
    rapidjson::Value* value_;
};

} // namespace doris
