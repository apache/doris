/*
 *  Copyright (c) 2014, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */

/*
 * This header defines JsonbDocument, JsonbKeyValue, and various value classes
 * which are derived from JsonbValue, and a forward iterator for container
 * values - essentially everything that is related to JSONB binary data
 * structures.
 *
 * Implementation notes:
 *
 * None of the classes in this header file can be instantiated directly (i.e.
 * you cannot create a JsonbKeyValue or JsonbValue object - all constructors
 * are declared non-public). We use the classes as wrappers on the packed JSONB
 * bytes (serialized), and cast the classes (types) to the underlying packed
 * byte array.
 *
 * For the same reason, we cannot define any JSONB value class to be virtual,
 * since we never call constructors, and will not instantiate vtbl and vptrs.
 *
 * Therefore, the classes are defined as packed structures (i.e. no data
 * alignment and padding), and the private member variables of the classes are
 * defined precisely in the same order as the JSONB spec. This ensures we
 * access the packed JSONB bytes correctly.
 *
 * The packed structures are highly optimized for in-place operations with low
 * overhead. The reads (and in-place writes) are performed directly on packed
 * bytes. There is no memory allocation at all at runtime.
 *
 * For updates/writes of values that will expand the original JSONB size, the
 * write will fail, and the caller needs to handle buffer increase.
 *
 * ** Iterator **
 * Both ObjectVal class and ArrayVal class have iterator type that you can use
 * to declare an iterator on a container object to go through the key-value
 * pairs or value list. The iterator has both non-const and const types.
 *
 * Note: iterators are forward direction only.
 *
 * ** Query **
 * Querying into containers is through the member functions find (for key/value
 * pairs) and get (for array elements), and is in streaming style. We don't
 * need to read/scan the whole JSONB packed bytes in order to return results.
 * Once the key/index is found, we will stop search.  You can use text to query
 * both objects and array (for array, text will be converted to integer index),
 * and use index to retrieve from array. Array index is 0-based.
 *
 * ** External dictionary **
 * During query processing, you can also pass a call-back function, so the
 * search will first try to check if the key string exists in the dictionary.
 * If so, search will be based on the id instead of the key string.
 * @author Tian Xia <tianx@fb.com>
 * 
 * this file is copied from 
 * https://github.com/facebook/mysql-5.6/blob/fb-mysql-5.6.35/fbson/FbsonDocument.h
 * and modified by Doris
 */

#ifndef JSONB_JSONBDOCUMENT_H
#define JSONB_JSONBDOCUMENT_H

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <limits>

// #include "util/string_parser.hpp"

namespace doris {

#pragma pack(push, 1)

#define JSONB_VER 1

// forward declaration
class JsonbValue;
class ObjectVal;

const int MaxNestingLevel = 100;

/*
 * JsonbType defines 10 primitive types and 2 container types, as described
 * below.
 *
 * primitive_value ::=
 *   0x00        //null value (0 byte)
 * | 0x01        //boolean true (0 byte)
 * | 0x02        //boolean false (0 byte)
 * | 0x03 int8   //char/int8 (1 byte)
 * | 0x04 int16  //int16 (2 bytes)
 * | 0x05 int32  //int32 (4 bytes)
 * | 0x06 int64  //int64 (8 bytes)
 * | 0x07 double //floating point (8 bytes)
 * | 0x08 string //variable length string
 * | 0x09 binary //variable length binary
 *
 * container ::=
 *   0x0A int32 key_value_list //object, int32 is the total bytes of the object
 * | 0x0B int32 value_list     //array, int32 is the total bytes of the array
 */
enum class JsonbType : char {
    T_Null = 0x00,
    T_True = 0x01,
    T_False = 0x02,
    T_Int8 = 0x03,
    T_Int16 = 0x04,
    T_Int32 = 0x05,
    T_Int64 = 0x06,
    T_Double = 0x07,
    T_String = 0x08,
    T_Binary = 0x09,
    T_Object = 0x0A,
    T_Array = 0x0B,
    NUM_TYPES,
};

/*
 * JsonbDocument is the main object that accesses and queries JSONB packed
 * bytes. NOTE: JsonbDocument only allows object container as the top level
 * JSONB value. However, you can use the static method "createValue" to get any
 * JsonbValue object from the packed bytes.
 *
 * JsonbDocument object also dereferences to an object container value
 * (ObjectVal) once JSONB is loaded.
 *
 * ** Load **
 * JsonbDocument is usable after loading packed bytes (memory location) into
 * the object. We only need the header and first few bytes of the payload after
 * header to verify the JSONB.
 *
 * Note: creating an JsonbDocument (through createDocument) does not allocate
 * any memory. The document object is an efficient wrapper on the packed bytes
 * which is accessed directly.
 *
 * ** Query **
 * Query is through dereferencing into ObjectVal.
 */
class JsonbDocument {
public:
    // Prepare a document in the buffer
    static JsonbDocument* makeDocument(char* pb, uint32_t size, JsonbType type);
    static JsonbDocument* makeDocument(char* pb, uint32_t size, const JsonbValue* rval);

    // create an JsonbDocument object from JSONB packed bytes
    static JsonbDocument* createDocument(const char* pb, uint32_t size);

    // create an JsonbValue from JSONB packed bytes
    static JsonbValue* createValue(const char* pb, uint32_t size);

    uint8_t version() { return header_.ver_; }

    JsonbValue* getValue() { return ((JsonbValue*)payload_); }

    void setValue(const JsonbValue* value);

    unsigned int numPackedBytes() const;

    ObjectVal* operator->() { return ((ObjectVal*)payload_); }

    const ObjectVal* operator->() const { return ((const ObjectVal*)payload_); }

public:
    bool operator==(const JsonbDocument& other) const {
        LOG(FATAL) << "comparing between JsonbDocument is not supported";
    }

    bool operator!=(const JsonbDocument& other) const {
        LOG(FATAL) << "comparing between JsonbDocument is not supported";
    }

    bool operator<=(const JsonbDocument& other) const {
        LOG(FATAL) << "comparing between JsonbDocument is not supported";
    }

    bool operator>=(const JsonbDocument& other) const {
        LOG(FATAL) << "comparing between JsonbDocument is not supported";
    }

    bool operator<(const JsonbDocument& other) const {
        LOG(FATAL) << "comparing between JsonbDocument is not supported";
    }

    bool operator>(const JsonbDocument& other) const {
        LOG(FATAL) << "comparing between JsonbDocument is not supported";
    }

private:
    /*
   * JsonbHeader class defines JSONB header (internal to JsonbDocument).
   *
   * Currently it only contains version information (1-byte). We may expand the
   * header to include checksum of the JSONB binary for more security.
   */
    struct JsonbHeader {
        uint8_t ver_;
    } header_;

    char payload_[0];
};

/*
 * JsonbFwdIteratorT implements JSONB's iterator template.
 *
 * Note: it is an FORWARD iterator only due to the design of JSONB format.
 */
template <class Iter_Type, class Cont_Type>
class JsonbFwdIteratorT {
public:
    typedef Iter_Type iterator;
    typedef typename std::iterator_traits<Iter_Type>::pointer pointer;
    typedef typename std::iterator_traits<Iter_Type>::reference reference;

public:
    explicit JsonbFwdIteratorT() : current_(nullptr) {}
    explicit JsonbFwdIteratorT(const iterator& i) : current_(i) {}

    // allow non-const to const iterator conversion (same container type)
    template <class Iter_Ty>
    JsonbFwdIteratorT(const JsonbFwdIteratorT<Iter_Ty, Cont_Type>& rhs) : current_(rhs.base()) {}

    bool operator==(const JsonbFwdIteratorT& rhs) const { return (current_ == rhs.current_); }

    bool operator!=(const JsonbFwdIteratorT& rhs) const { return !operator==(rhs); }

    bool operator<(const JsonbFwdIteratorT& rhs) const { return (current_ < rhs.current_); }

    bool operator>(const JsonbFwdIteratorT& rhs) const { return !operator<(rhs); }

    JsonbFwdIteratorT& operator++() {
        current_ = (iterator)(((char*)current_) + current_->numPackedBytes());
        return *this;
    }

    JsonbFwdIteratorT operator++(int) {
        auto tmp = *this;
        current_ = (iterator)(((char*)current_) + current_->numPackedBytes());
        return tmp;
    }

    explicit operator pointer() { return current_; }

    reference operator*() const { return *current_; }

    pointer operator->() const { return current_; }

    iterator base() const { return current_; }

private:
    iterator current_;
};

typedef int (*hDictInsert)(const char* key, unsigned len);
typedef int (*hDictFind)(const char* key, unsigned len);

typedef std::underlying_type<JsonbType>::type JsonbTypeUnder;

/*
 * JsonbKeyValue class defines JSONB key type, as described below.
 *
 * key ::=
 *   0x00 int8    //1-byte dictionary id
 * | int8 (byte*) //int8 (>0) is the size of the key string
 *
 * value ::= primitive_value | container
 *
 * JsonbKeyValue can be either an id mapping to the key string in an external
 * dictionary, or it is the original key string. Whether to read an id or a
 * string is decided by the first byte (size_).
 *
 * Note: a key object must be followed by a value object. Therefore, a key
 * object implicitly refers to a key-value pair, and you can get the value
 * object right after the key object. The function numPackedBytes hence
 * indicates the total size of the key-value pair, so that we will be able go
 * to next pair from the key.
 *
 * ** Dictionary size **
 * By default, the dictionary size is 255 (1-byte). Users can define
 * "USE_LARGE_DICT" to increase the dictionary size to 655535 (2-byte).
 */
class JsonbKeyValue {
public:
#ifdef USE_LARGE_DICT
    static const int sMaxKeyId = 65535;
    typedef uint16_t keyid_type;
#else
    static const int sMaxKeyId = 255;
    typedef uint8_t keyid_type;
#endif // #ifdef USE_LARGE_DICT

    static const uint8_t sMaxKeyLen = 64;

    // size of the key. 0 indicates it is stored as id
    uint8_t klen() const { return size_; }

    // get the key string. Note the string may not be null terminated.
    const char* getKeyStr() const { return key_.str_; }

    keyid_type getKeyId() const { return key_.id_; }

    unsigned int keyPackedBytes() const {
        return size_ ? (sizeof(size_) + size_) : (sizeof(size_) + sizeof(keyid_type));
    }

    JsonbValue* value() const { return (JsonbValue*)(((char*)this) + keyPackedBytes()); }

    // size of the total packed bytes (key+value)
    unsigned int numPackedBytes() const;

private:
    uint8_t size_;

    union key_ {
        keyid_type id_;
        char str_[1];
    } key_;

    JsonbKeyValue();
};

/*
 * JsonbValue is the base class of all JSONB types. It contains only one member
 * variable - type info, which can be retrieved by member functions is[Type]()
 * or type().
 */
class JsonbValue {
public:
    static const uint32_t sMaxValueLen = 1 << 24; // 16M

    bool isNull() const { return (type_ == JsonbType::T_Null); }
    bool isTrue() const { return (type_ == JsonbType::T_True); }
    bool isFalse() const { return (type_ == JsonbType::T_False); }
    bool isInt() const { return isInt8() || isInt16() || isInt32() || isInt64(); }
    bool isInt8() const { return (type_ == JsonbType::T_Int8); }
    bool isInt16() const { return (type_ == JsonbType::T_Int16); }
    bool isInt32() const { return (type_ == JsonbType::T_Int32); }
    bool isInt64() const { return (type_ == JsonbType::T_Int64); }
    bool isDouble() const { return (type_ == JsonbType::T_Double); }
    bool isString() const { return (type_ == JsonbType::T_String); }
    bool isBinary() const { return (type_ == JsonbType::T_Binary); }
    bool isObject() const { return (type_ == JsonbType::T_Object); }
    bool isArray() const { return (type_ == JsonbType::T_Array); }

    JsonbType type() const { return type_; }

    const char* typeName() const {
        switch (type_) {
        case JsonbType::T_Null:
            return "null";
        case JsonbType::T_True:
        case JsonbType::T_False:
            return "bool";
        case JsonbType::T_Int8:
        case JsonbType::T_Int16:
        case JsonbType::T_Int32:
            return "int";
        case JsonbType::T_Int64:
            return "bigint";
        case JsonbType::T_Double:
            return "double";
        case JsonbType::T_String:
            return "string";
        case JsonbType::T_Binary:
            return "binary";
        case JsonbType::T_Object:
            return "object";
        case JsonbType::T_Array:
            return "array";
        default:
            return "unknown";
        }
    }

    // size of the total packed bytes
    unsigned int numPackedBytes() const;

    // size of the value in bytes
    unsigned int size() const;

    // get the raw byte array of the value
    const char* getValuePtr() const;

    // find the JSONB value by a key path string (null terminated)
    JsonbValue* findPath(const char* key_path, const char* delim = ".",
                         hDictFind handler = nullptr) {
        return findPath(key_path, (unsigned int)strlen(key_path), delim, handler);
    }

    // find the JSONB value by a key path string (with length)
    JsonbValue* findPath(const char* key_path, unsigned int len, const char* delim,
                         hDictFind handler);
    friend class JsonbDocument;

protected:
    JsonbType type_; // type info

    JsonbValue();
};

/*
 * NumerValT is the template class (derived from JsonbValue) of all number
 * types (integers and double).
 */
template <class T>
class NumberValT : public JsonbValue {
public:
    T val() const { return num_; }

    unsigned int numPackedBytes() const { return sizeof(JsonbValue) + sizeof(T); }

    // catch all unknow specialization of the template class
    bool setVal(T value) { return false; }

private:
    T num_;

    NumberValT();
};

typedef NumberValT<int8_t> JsonbInt8Val;

// override setVal for Int8Val
template <>
inline bool JsonbInt8Val::setVal(int8_t value) {
    if (!isInt8()) {
        return false;
    }

    num_ = value;
    return true;
}

typedef NumberValT<int16_t> JsonbInt16Val;

// override setVal for Int16Val
template <>
inline bool JsonbInt16Val::setVal(int16_t value) {
    if (!isInt16()) {
        return false;
    }

    num_ = value;
    return true;
}
typedef NumberValT<int32_t> JsonbInt32Val;

// override setVal for Int32Val
template <>
inline bool JsonbInt32Val::setVal(int32_t value) {
    if (!isInt32()) {
        return false;
    }

    num_ = value;
    return true;
}

typedef NumberValT<int64_t> JsonbInt64Val;

// override setVal for Int64Val
template <>
inline bool JsonbInt64Val::setVal(int64_t value) {
    if (!isInt64()) {
        return false;
    }

    num_ = value;
    return true;
}

typedef NumberValT<double> JsonbDoubleVal;

// override setVal for DoubleVal
template <>
inline bool JsonbDoubleVal::setVal(double value) {
    if (!isDouble()) {
        return false;
    }

    num_ = value;
    return true;
}

// A class to get an integer
class JsonbIntVal : public JsonbValue {
public:
    int64_t val() const {
        switch (type_) {
        case JsonbType::T_Int8:
            return ((JsonbInt8Val*)this)->val();
        case JsonbType::T_Int16:
            return ((JsonbInt16Val*)this)->val();
        case JsonbType::T_Int32:
            return ((JsonbInt32Val*)this)->val();
        case JsonbType::T_Int64:
            return ((JsonbInt64Val*)this)->val();
        default:
            return 0;
        }
    }
    bool setVal(int64_t val) {
        switch (type_) {
        case JsonbType::T_Int8:
            if (val < std::numeric_limits<int8_t>::min() ||
                val > std::numeric_limits<int8_t>::max())
                return false;
            return ((JsonbInt8Val*)this)->setVal((int8_t)val);
        case JsonbType::T_Int16:
            if (val < std::numeric_limits<int16_t>::min() ||
                val > std::numeric_limits<int16_t>::max())
                return false;
            return ((JsonbInt16Val*)this)->setVal((int16_t)val);
        case JsonbType::T_Int32:
            if (val < std::numeric_limits<int32_t>::min() ||
                val > std::numeric_limits<int32_t>::max())
                return false;
            return ((JsonbInt32Val*)this)->setVal((int32_t)val);
        case JsonbType::T_Int64:
            return ((JsonbInt64Val*)this)->setVal(val);
        default:
            return false;
        }
    }
};

/*
 * BlobVal is the base class (derived from JsonbValue) for string and binary
 * types. The size_ indicates the total bytes of the payload_.
 */
class JsonbBlobVal : public JsonbValue {
public:
    // size of the blob payload only
    unsigned int getBlobLen() const { return size_; }

    // return the blob as byte array
    const char* getBlob() const { return payload_; }

    // size of the total packed bytes
    unsigned int numPackedBytes() const { return sizeof(JsonbValue) + sizeof(size_) + size_; }
    friend class JsonbDocument;

protected:
    uint32_t size_;
    char payload_[0];

    // set new blob bytes
    bool internalSetVal(const char* blob, uint32_t blobSize) {
        // if we cannot fit the new blob, fail the operation
        if (blobSize > size_) {
            return false;
        }

        memcpy(payload_, blob, blobSize);

        // Set the reset of the bytes to 0.  Note we cannot change the size_ of the
        // current payload, as all values are packed.
        memset(payload_ + blobSize, 0, size_ - blobSize);

        return true;
    }

    JsonbBlobVal();
};

/*
 * Binary type
 */
class JsonbBinaryVal : public JsonbBlobVal {
public:
    bool setVal(const char* blob, uint32_t blobSize) {
        if (!isBinary()) {
            return false;
        }

        return internalSetVal(blob, blobSize);
    }

private:
    JsonbBinaryVal();
};

/*
 * String type
 * Note: JSONB string may not be a c-string (NULL-terminated)
 */
class JsonbStringVal : public JsonbBlobVal {
public:
    bool setVal(const char* str, uint32_t blobSize) {
        if (!isString()) {
            return false;
        }

        return internalSetVal(str, blobSize);
    }
    /*
    This function return the actual size of a string. Since for
    a string, it can be null-terminated with null paddings or it
    can take all the space in the payload_ without null in the end.
    So we need to check it to get the true actual length of a string.
  */
    size_t length() {
        // It's an empty string
        if (0 == size_) return size_;
        // The string stored takes all the spaces in payload_
        if (payload_[size_ - 1] != 0) {
            return size_;
        }
        // It's shorter than the size of payload_
        return strnlen(payload_, size_);
    }
    // convert the string (case insensitive) to a boolean value
    // "false": 0
    // "true": 1
    // all other strings: -1
    int getBoolVal() {
        if (size_ == 4 && tolower(payload_[0]) == 't' && tolower(payload_[1]) == 'r' &&
            tolower(payload_[2]) == 'u' && tolower(payload_[3]) == 'e')
            return 1;
        else if (size_ == 5 && tolower(payload_[0]) == 'f' && tolower(payload_[1]) == 'a' &&
                 tolower(payload_[2]) == 'l' && tolower(payload_[3]) == 's' &&
                 tolower(payload_[4]) == 'e')
            return 0;
        else
            return -1;
    }

private:
    JsonbStringVal();
};

/*
 * ContainerVal is the base class (derived from JsonbValue) for object and
 * array types. The size_ indicates the total bytes of the payload_.
 */
class ContainerVal : public JsonbValue {
public:
    // size of the container payload only
    unsigned int getContainerSize() const { return size_; }

    // return the container payload as byte array
    const char* getPayload() const { return payload_; }

    // size of the total packed bytes
    unsigned int numPackedBytes() const { return sizeof(JsonbValue) + sizeof(size_) + size_; }
    friend class JsonbDocument;

protected:
    uint32_t size_;
    char payload_[0];

    ContainerVal();
};

/*
 * Object type
 */
class ObjectVal : public ContainerVal {
public:
    typedef JsonbKeyValue value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef JsonbFwdIteratorT<pointer, ObjectVal> iterator;
    typedef JsonbFwdIteratorT<const_pointer, ObjectVal> const_iterator;

public:
    const_iterator search(const char* key, hDictFind handler = nullptr) const {
        return const_cast<ObjectVal*>(this)->search(key, handler);
    }

    const_iterator search(const char* key, unsigned int klen, hDictFind handler = nullptr) const {
        return const_cast<ObjectVal*>(this)->search(key, klen, handler);
    }

    const_iterator search(int key_id) const { return const_cast<ObjectVal*>(this)->search(key_id); }
    iterator search(const char* key, hDictFind handler = nullptr) {
        if (!key) {
            return end();
        }
        return search(key, (unsigned int)strlen(key), handler);
    }

    iterator search(const char* key, unsigned int klen, hDictFind handler = nullptr) {
        if (!key || !klen) return end();

        int key_id = -1;
        if (handler && (key_id = handler(key, klen)) >= 0) {
            return search(key_id);
        }
        return internalSearch(key, klen);
    }

    iterator search(int key_id) {
        if (key_id < 0 || key_id > JsonbKeyValue::sMaxKeyId) return end();

        const char* pch = payload_;
        const char* fence = payload_ + size_;

        while (pch < fence) {
            JsonbKeyValue* pkey = (JsonbKeyValue*)(pch);
            if (!pkey->klen() && key_id == pkey->getKeyId()) {
                return iterator(pkey);
            }
            pch += pkey->numPackedBytes();
        }

        assert(pch == fence);
        return end();
    }

    JsonbValue* find(const char* key, hDictFind handler = nullptr) const {
        return const_cast<ObjectVal*>(this)->find(key, handler);
    }

    JsonbValue* find(const char* key, unsigned int klen, hDictFind handler = nullptr) const {
        return const_cast<ObjectVal*>(this)->find(key, klen, handler);
    }
    JsonbValue* find(int key_id) const { return const_cast<ObjectVal*>(this)->find(key_id); }

    // find the JSONB value by a key string (null terminated)
    JsonbValue* find(const char* key, hDictFind handler = nullptr) {
        if (!key) return nullptr;
        return find(key, (unsigned int)strlen(key), handler);
    }

    // find the JSONB value by a key string (with length)
    JsonbValue* find(const char* key, unsigned int klen, hDictFind handler = nullptr) {
        iterator kv = search(key, klen, handler);
        if (end() == kv) return nullptr;
        return kv->value();
    }

    // find the JSONB value by a key dictionary ID
    JsonbValue* find(int key_id) {
        iterator kv = search(key_id);
        if (end() == kv) return nullptr;
        return kv->value();
    }

    iterator begin() { return iterator((pointer)payload_); }

    const_iterator begin() const { return const_iterator((pointer)payload_); }

    iterator end() { return iterator((pointer)(payload_ + size_)); }

    const_iterator end() const { return const_iterator((pointer)(payload_ + size_)); }

private:
    iterator internalSearch(const char* key, unsigned int klen) {
        const char* pch = payload_;
        const char* fence = payload_ + size_;

        while (pch < fence) {
            JsonbKeyValue* pkey = (JsonbKeyValue*)(pch);
            if (klen == pkey->klen() && strncmp(key, pkey->getKeyStr(), klen) == 0) {
                return iterator(pkey);
            }
            pch += pkey->numPackedBytes();
        }

        assert(pch == fence);

        return end();
    }

private:
    ObjectVal();
};

/*
 * Array type
 */
class ArrayVal : public ContainerVal {
public:
    // get the JSONB value at index
    JsonbValue* get(int idx) const {
        if (idx < 0) return nullptr;

        const char* pch = payload_;
        const char* fence = payload_ + size_;

        while (pch < fence && idx-- > 0) pch += ((JsonbValue*)pch)->numPackedBytes();
        if (idx > 0 || pch == fence) return nullptr;

        return (JsonbValue*)pch;
    }

    // Get number of elements in array
    unsigned int numElem() const {
        const char* pch = payload_;
        const char* fence = payload_ + size_;

        unsigned int num = 0;
        while (pch < fence) {
            ++num;
            pch += ((JsonbValue*)pch)->numPackedBytes();
        }

        assert(pch == fence);

        return num;
    }

    typedef JsonbValue value_type;
    typedef value_type* pointer;
    typedef const value_type* const_pointer;
    typedef JsonbFwdIteratorT<pointer, ArrayVal> iterator;
    typedef JsonbFwdIteratorT<const_pointer, ArrayVal> const_iterator;

    iterator begin() { return iterator((pointer)payload_); }

    const_iterator begin() const { return const_iterator((pointer)payload_); }

    iterator end() { return iterator((pointer)(payload_ + size_)); }

    const_iterator end() const { return const_iterator((pointer)(payload_ + size_)); }

private:
    ArrayVal();
};

// Prepare an empty document
// input: pb - buuffer/packed bytes for jsonb document
//        size - size of the buffer
//        type - value type in the document
inline JsonbDocument* JsonbDocument::makeDocument(char* pb, uint32_t size, JsonbType type) {
    if (!pb || size < sizeof(JsonbHeader) + sizeof(JsonbValue)) {
        return nullptr;
    }

    if (type < JsonbType::T_Null || type >= JsonbType::NUM_TYPES) {
        return nullptr;
    }
    JsonbDocument* doc = (JsonbDocument*)pb;
    // Write header
    doc->header_.ver_ = JSONB_VER;
    JsonbValue* value = doc->getValue();
    // Write type
    value->type_ = type;

    // Set empty JsonbValue
    if (type == JsonbType::T_Object || type == JsonbType::T_Array)
        ((ContainerVal*)value)->size_ = 0;
    if (type == JsonbType::T_String || type == JsonbType::T_Binary)
        ((JsonbBlobVal*)value)->size_ = 0;
    return doc;
}

// Prepare a document from an JsonbValue
// input: pb - buuffer/packed bytes for jsonb document
//        size - size of the buffer
//        rval - jsonb value to be copied into the document
inline JsonbDocument* JsonbDocument::makeDocument(char* pb, uint32_t size, const JsonbValue* rval) {
    // checking if the buffer is big enough to store the value
    if (!pb || !rval || size < sizeof(JsonbHeader) + rval->numPackedBytes()) {
        return nullptr;
    }

    JsonbType type = rval->type();
    if (type < JsonbType::T_Null || type >= JsonbType::NUM_TYPES) {
        return nullptr;
    }
    JsonbDocument* doc = (JsonbDocument*)pb;
    // Write header
    doc->header_.ver_ = JSONB_VER;
    // get the starting byte of the value
    JsonbValue* value = doc->getValue();
    // binary copy of the rval
    if (value != rval) // copy not necessary if values are the same
        memmove(value, rval, rval->numPackedBytes());

    return doc;
}

inline JsonbDocument* JsonbDocument::createDocument(const char* pb, uint32_t size) {
    if (!pb || size < sizeof(JsonbHeader) + sizeof(JsonbValue)) {
        return nullptr;
    }

    JsonbDocument* doc = (JsonbDocument*)pb;
    if (doc->header_.ver_ != JSONB_VER) {
        return nullptr;
    }

    JsonbValue* val = (JsonbValue*)doc->payload_;
    if (val->type() < JsonbType::T_Null || val->type() >= JsonbType::NUM_TYPES ||
        size != sizeof(JsonbHeader) + val->numPackedBytes()) {
        return nullptr;
    }

    return doc;
}
inline void JsonbDocument::setValue(const JsonbValue* value) {
    memcpy(payload_, value, value->numPackedBytes());
}

inline JsonbValue* JsonbDocument::createValue(const char* pb, uint32_t size) {
    if (!pb || size < sizeof(JsonbHeader) + sizeof(JsonbValue)) {
        return nullptr;
    }

    JsonbDocument* doc = (JsonbDocument*)pb;
    if (doc->header_.ver_ != JSONB_VER) {
        return nullptr;
    }

    JsonbValue* val = (JsonbValue*)doc->payload_;
    if (size != sizeof(JsonbHeader) + val->numPackedBytes()) {
        return nullptr;
    }

    return val;
}

inline unsigned int JsonbDocument::numPackedBytes() const {
    return ((const JsonbValue*)payload_)->numPackedBytes() + sizeof(header_);
}

inline unsigned int JsonbKeyValue::numPackedBytes() const {
    unsigned int ks = keyPackedBytes();
    JsonbValue* val = (JsonbValue*)(((char*)this) + ks);
    return ks + val->numPackedBytes();
}

// Poor man's "virtual" function JsonbValue::numPackedBytes
inline unsigned int JsonbValue::numPackedBytes() const {
    switch (type_) {
    case JsonbType::T_Null:
    case JsonbType::T_True:
    case JsonbType::T_False: {
        return sizeof(type_);
    }

    case JsonbType::T_Int8: {
        return sizeof(type_) + sizeof(int8_t);
    }
    case JsonbType::T_Int16: {
        return sizeof(type_) + sizeof(int16_t);
    }
    case JsonbType::T_Int32: {
        return sizeof(type_) + sizeof(int32_t);
    }
    case JsonbType::T_Int64: {
        return sizeof(type_) + sizeof(int64_t);
    }
    case JsonbType::T_Double: {
        return sizeof(type_) + sizeof(double);
    }
    case JsonbType::T_String:
    case JsonbType::T_Binary: {
        return ((JsonbBlobVal*)(this))->numPackedBytes();
    }

    case JsonbType::T_Object:
    case JsonbType::T_Array: {
        return ((ContainerVal*)(this))->numPackedBytes();
    }
    default:
        return 0;
    }
}

inline unsigned int JsonbValue::size() const {
    switch (type_) {
    case JsonbType::T_Int8: {
        return sizeof(int8_t);
    }
    case JsonbType::T_Int16: {
        return sizeof(int16_t);
    }
    case JsonbType::T_Int32: {
        return sizeof(int32_t);
    }
    case JsonbType::T_Int64: {
        return sizeof(int64_t);
    }
    case JsonbType::T_Double: {
        return sizeof(double);
    }
    case JsonbType::T_String:
    case JsonbType::T_Binary: {
        return ((JsonbBlobVal*)(this))->getBlobLen();
    }

    case JsonbType::T_Object:
    case JsonbType::T_Array: {
        return ((ContainerVal*)(this))->getContainerSize();
    }
    case JsonbType::T_Null:
    case JsonbType::T_True:
    case JsonbType::T_False:
    default:
        return 0;
    }
}

inline const char* JsonbValue::getValuePtr() const {
    switch (type_) {
    case JsonbType::T_Int8:
    case JsonbType::T_Int16:
    case JsonbType::T_Int32:
    case JsonbType::T_Int64:
    case JsonbType::T_Double:
        return ((char*)this) + sizeof(JsonbType);

    case JsonbType::T_String:
    case JsonbType::T_Binary:
        return ((JsonbBlobVal*)(this))->getBlob();

    case JsonbType::T_Object:
    case JsonbType::T_Array:
        return ((ContainerVal*)(this))->getPayload();

    case JsonbType::T_Null:
    case JsonbType::T_True:
    case JsonbType::T_False:
    default:
        return nullptr;
    }
}

inline JsonbValue* JsonbValue::findPath(const char* key_path, unsigned int kp_len,
                                        const char* delim = ".", hDictFind handler = nullptr) {
    if (!key_path) return nullptr;
    if (kp_len == 0) return this;

    // skip $ and . at beginning
    if (kp_len > 0 && *key_path == '$') {
        key_path++;
        kp_len--;
        if (kp_len > 0 && *key_path == '.') {
            key_path++;
            kp_len--;
        }
    }

    if (kp_len == 0) return this;

    if (!delim) delim = "."; // default delimiter

    JsonbValue* pval = this;
    const char* fence = key_path + kp_len;
    char idx_buf[21]; // buffer to parse array index (integer value)

    while (pval && key_path < fence) {
        const char* key = key_path;
        unsigned int klen = 0;
        const char* left_bracket = nullptr;
        const char* right_bracket = nullptr;
        size_t idx_len = 0;
        // find the current key and [] bracket position
        for (; key_path != fence && *key_path != *delim; ++key_path, ++klen) {
            if ('[' == *key_path) {
                left_bracket = key_path;
            } else if (']' == *key_path) {
                right_bracket = key_path;
            }
        }

        // check brackets and array index length
        if (left_bracket || right_bracket) {
            if (!left_bracket || !right_bracket) {
                return nullptr;
            }
            // check the last char is ]
            if (key + klen - 1 != right_bracket) {
                return nullptr;
            }
            // the part before left_bracket is object key
            klen = left_bracket - key;
            // the part between left_bracket and right_bracket is array index
            idx_len = right_bracket - left_bracket - 1;
        }

        if (!klen && !idx_len) return nullptr;

        // get value of key in object
        if (klen) {
            if (LIKELY(pval->type_ == JsonbType::T_Object)) {
                pval = ((ObjectVal*)pval)->find(key, klen, handler);
                if (!pval) return nullptr;
            } else {
                return nullptr;
            }
        }

        // get value at idx in array
        if (idx_len) {
            if (LIKELY(pval->type_ == JsonbType::T_Array)) {
                if (idx_len >= sizeof(idx_buf)) return nullptr;
                memcpy(idx_buf, left_bracket + 1, idx_len);
                idx_buf[idx_len] = 0;

                char* end = nullptr;
                int index = (int)strtol(idx_buf, &end, 10);
                if (end && !*end)
                    pval = ((ArrayVal*)pval)->get(index);
                else
                    // incorrect index string
                    return nullptr;

                // doris::StringParser::ParseResult parse_result;
                // int index = doris::StringParser::string_to_int<int>(left_bracket + 1, idx_len, &parse_result);
                // if (parse_result == doris::StringParser::ParseResult::PARSE_SUCCESS)
            } else {
                return nullptr;
            }
        }

        // skip the delimiter
        if (key_path < fence) {
            ++key_path;
            if (key_path == fence)
                // we have a trailing delimiter at the end
                return nullptr;
        }
    }

    return pval;
}

#pragma pack(pop)

} // namespace doris

#endif // JSONB_JSONBDOCUMENT_H
