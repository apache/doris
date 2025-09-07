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

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "util/string_util.h"
#include "vec/core/types.h"

// #include "util/string_parser.hpp"

// Concept to check for supported decimal types
template <typename T>
concept JsonbDecimalType = std::same_as<T, doris::vectorized::Decimal256> ||
                           std::same_as<T, doris::vectorized::Decimal64> ||
                           std::same_as<T, doris::vectorized::Decimal128V3> ||
                           std::same_as<T, doris::vectorized::Decimal32>;

namespace doris {

template <typename T>
constexpr bool is_pod_v = std::is_trivial_v<T> && std::is_standard_layout_v<T>;

struct JsonbStringVal;
struct ObjectVal;
struct ArrayVal;
struct JsonbBinaryVal;
struct ContainerVal;

template <JsonbDecimalType T>
struct JsonbDecimalVal;

using JsonbDecimal256 = JsonbDecimalVal<vectorized::Decimal256>;
using JsonbDecimal128 = JsonbDecimalVal<vectorized::Decimal128V3>;
using JsonbDecimal64 = JsonbDecimalVal<vectorized::Decimal64>;
using JsonbDecimal32 = JsonbDecimalVal<vectorized::Decimal32>;

template <typename T>
    requires std::is_integral_v<T> || std::is_floating_point_v<T>
struct NumberValT;

using JsonbInt8Val = NumberValT<int8_t>;
using JsonbInt16Val = NumberValT<int16_t>;
using JsonbInt32Val = NumberValT<int32_t>;
using JsonbInt64Val = NumberValT<int64_t>;
using JsonbInt128Val = NumberValT<int128_t>;
using JsonbDoubleVal = NumberValT<double>;
using JsonbFloatVal = NumberValT<float>;

template <typename T>
concept JsonbPodType = (std::same_as<T, JsonbStringVal> || std::same_as<T, ObjectVal> ||
                        std::same_as<T, ContainerVal> || std::same_as<T, ArrayVal> ||
                        std::same_as<T, JsonbBinaryVal> || std::same_as<T, JsonbDecimal32> ||
                        std::same_as<T, JsonbDecimal64> || std::same_as<T, JsonbDecimal128> ||
                        std::same_as<T, JsonbDecimal256> || std::same_as<T, JsonbDecimal32> ||
                        std::same_as<T, JsonbInt8Val> || std::same_as<T, JsonbInt16Val> ||
                        std::same_as<T, JsonbInt32Val> || std::same_as<T, JsonbInt64Val> ||
                        std::same_as<T, JsonbInt128Val> || std::same_as<T, JsonbFloatVal> ||
                        std::same_as<T, JsonbFloatVal> || std::same_as<T, JsonbDoubleVal>);

#define JSONB_VER 1

using int128_t = __int128;

// forward declaration
struct JsonbValue;

class JsonbOutStream;

template <class OS_TYPE>
class JsonbWriterT;

using JsonbWriter = JsonbWriterT<JsonbOutStream>;

const int MaxNestingLevel = 100;

/*
 * JsonbType defines 10 primitive types and 2 container types, as described
 * below.
 * NOTE: Do NOT modify the existing values or their order in this enum.
 *      You may only append new entries at the end before `NUM_TYPES`.
 *      This enum will be used in serialized data and/or persisted data.
 *      Changing existing values may break backward compatibility
 *      with previously stored or transmitted data.
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
    T_Int128 = 0x0C,
    T_Float = 0x0D,
    T_Decimal32 = 0x0E,  // DecimalV3 only
    T_Decimal64 = 0x0F,  // DecimalV3 only
    T_Decimal128 = 0x10, // DecimalV3 only
    T_Decimal256 = 0x11, // DecimalV3 only
    NUM_TYPES,
};

inline PrimitiveType get_primitive_type_from_json_type(JsonbType json_type) {
    switch (json_type) {
    case JsonbType::T_Null:
        return TYPE_NULL;
    case JsonbType::T_True:
    case JsonbType::T_False:
        return TYPE_BOOLEAN;
    case JsonbType::T_Int8:
        return TYPE_TINYINT;
    case JsonbType::T_Int16:
        return TYPE_SMALLINT;
    case JsonbType::T_Int32:
        return TYPE_INT;
    case JsonbType::T_Int64:
        return TYPE_BIGINT;
    case JsonbType::T_Double:
        return TYPE_DOUBLE;
    case JsonbType::T_String:
        return TYPE_STRING;
    case JsonbType::T_Binary:
        return TYPE_BINARY;
    case JsonbType::T_Object:
        return TYPE_STRUCT;
    case JsonbType::T_Array:
        return TYPE_ARRAY;
    case JsonbType::T_Int128:
        return TYPE_LARGEINT;
    case JsonbType::T_Float:
        return TYPE_FLOAT;
    case JsonbType::T_Decimal32:
        return TYPE_DECIMAL32;
    case JsonbType::T_Decimal64:
        return TYPE_DECIMAL64;
    case JsonbType::T_Decimal128:
        return TYPE_DECIMAL128I;
    case JsonbType::T_Decimal256:
        return TYPE_DECIMAL256;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "Unsupported JsonbType: {}",
                        static_cast<int>(json_type));
    }
}

//for parse json path
constexpr char SCOPE = '$';
constexpr char BEGIN_MEMBER = '.';
constexpr char BEGIN_ARRAY = '[';
constexpr char END_ARRAY = ']';
constexpr char DOUBLE_QUOTE = '"';
constexpr char WILDCARD = '*';
constexpr char MINUS = '-';
constexpr char LAST[] = "last";
constexpr char ESCAPE = '\\';
constexpr unsigned int MEMBER_CODE = 0;
constexpr unsigned int ARRAY_CODE = 1;

/// A simple input stream class for the JSON path parser.
class Stream {
public:
    /// Creates an input stream reading from a character string.
    /// @param string  the input string
    /// @param length  the length of the input string
    Stream(const char* string, size_t length) : m_position(string), m_end(string + length) {}

    /// Returns a pointer to the current position in the stream.
    const char* position() const { return m_position; }

    /// Returns a pointer to the position just after the end of the stream.
    const char* end() const { return m_end; }

    /// Returns the number of bytes remaining in the stream.
    size_t remaining() const {
        assert(m_position <= m_end);
        return m_end - m_position;
    }

    /// Tells if the stream has been exhausted.
    bool exhausted() const { return remaining() == 0; }

    /// Reads the next byte from the stream and moves the position forward.
    char read() {
        assert(!exhausted());
        return *m_position++;
    }

    /// Reads the next byte from the stream without moving the position forward.
    char peek() const {
        assert(!exhausted());
        return *m_position;
    }

    /// Moves the position to the next non-whitespace character.
    void skip_whitespace() {
        m_position = std::find_if_not(m_position, m_end, [](char c) { return std::isspace(c); });
    }

    /// Moves the position n bytes forward.
    void skip(size_t n) {
        assert(remaining() >= n);
        m_position += n;
        skip_whitespace();
    }

    void advance() { m_position++; }

    void clear_leg_ptr() { leg_ptr = nullptr; }

    void set_leg_ptr(char* ptr) {
        clear_leg_ptr();
        leg_ptr = ptr;
    }

    char* get_leg_ptr() { return leg_ptr; }

    void clear_leg_len() { leg_len = 0; }

    void add_leg_len() { leg_len++; }

    unsigned int get_leg_len() const { return leg_len; }

    void remove_escapes() {
        int new_len = 0;
        for (int i = 0; i < leg_len; i++) {
            if (leg_ptr[i] != '\\') {
                leg_ptr[new_len++] = leg_ptr[i];
            }
        }
        leg_ptr[new_len] = '\0';
        leg_len = new_len;
    }

    void set_has_escapes(bool has) { has_escapes = has; }

    bool get_has_escapes() const { return has_escapes; }

private:
    /// The current position in the stream.
    const char* m_position = nullptr;

    /// The end of the stream.
    const char* const m_end;

    ///path leg ptr
    char* leg_ptr = nullptr;

    ///path leg len
    unsigned int leg_len;

    ///Whether to contain escape characters
    bool has_escapes = false;
};

struct leg_info {
    ///path leg ptr
    char* leg_ptr = nullptr;

    ///path leg len
    unsigned int leg_len;

    ///array_index
    int array_index;

    ///type: 0 is member 1 is array
    unsigned int type;

    bool to_string(std::string* str) const {
        if (type == MEMBER_CODE) {
            str->push_back(BEGIN_MEMBER);
            bool contains_space = false;
            std::string tmp;
            for (auto* it = leg_ptr; it != (leg_ptr + leg_len); ++it) {
                if (std::isspace(*it)) {
                    contains_space = true;
                } else if (*it == '"' || *it == ESCAPE || *it == '\r' || *it == '\n' ||
                           *it == '\b' || *it == '\t') {
                    tmp.push_back(ESCAPE);
                }
                tmp.push_back(*it);
            }
            if (contains_space) {
                str->push_back(DOUBLE_QUOTE);
            }
            str->append(tmp);
            if (contains_space) {
                str->push_back(DOUBLE_QUOTE);
            }
            return true;
        } else if (type == ARRAY_CODE) {
            str->push_back(BEGIN_ARRAY);
            std::string int_str = std::to_string(array_index);
            str->append(int_str);
            str->push_back(END_ARRAY);
            return true;
        } else {
            return false;
        }
    }
};

class JsonbPath {
public:
    // parse json path
    static bool parsePath(Stream* stream, JsonbPath* path);

    static bool parse_array(Stream* stream, JsonbPath* path);
    static bool parse_member(Stream* stream, JsonbPath* path);

    //return true if json path valid else return false
    bool seek(const char* string, size_t length);

    void add_leg_to_leg_vector(std::unique_ptr<leg_info> leg) {
        leg_vector.emplace_back(leg.release());
    }

    void pop_leg_from_leg_vector() { leg_vector.pop_back(); }

    bool to_string(std::string* res) const {
        res->push_back(SCOPE);
        for (const auto& leg : leg_vector) {
            auto valid = leg->to_string(res);
            if (!valid) {
                return false;
            }
        }
        return true;
    }

    size_t get_leg_vector_size() const { return leg_vector.size(); }

    leg_info* get_leg_from_leg_vector(size_t i) const { return leg_vector[i].get(); }

    bool is_wildcard() const { return _is_wildcard; }
    bool is_supper_wildcard() const { return _is_supper_wildcard; }

    void clean() { leg_vector.clear(); }

private:
    std::vector<std::unique_ptr<leg_info>> leg_vector;
    bool _is_wildcard = false;        // whether the path is a wildcard path
    bool _is_supper_wildcard = false; // supper wildcard likes '$**.a' or '$**[1]'
};

/*
 * JsonbFwdIteratorT implements JSONB's iterator template.
 *
 * Note: it is an FORWARD iterator only due to the design of JSONB format.
 */
template <class Iter_Type, class Cont_Type>
class JsonbFwdIteratorT {
public:
    using iterator = Iter_Type;
    using pointer = typename std::iterator_traits<Iter_Type>::pointer;
    using reference = typename std::iterator_traits<Iter_Type>::reference;

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

using hDictInsert = int (*)(const char*, unsigned int);
using hDictFind = int (*)(const char*, unsigned int);

using JsonbTypeUnder = std::underlying_type_t<JsonbType>;

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-length-array"
#endif
#pragma pack(push, 1)

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
    // create an JsonbDocument object from JSONB packed bytes
    [[nodiscard]] static Status checkAndCreateDocument(const char* pb, size_t size,
                                                       JsonbDocument** doc);

    // create an JsonbValue from JSONB packed bytes
    static JsonbValue* createValue(const char* pb, size_t size);

    uint8_t version() const { return header_.ver_; }

    JsonbValue* getValue() { return ((JsonbValue*)payload_); }

    void setValue(const JsonbValue* value);

    unsigned int numPackedBytes() const;

    // ObjectVal* operator->();

    const ObjectVal* operator->() const;

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
 * string is decided by the first byte (size).
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
    // now we use sMaxKeyId to represent an empty key
    static const int sMaxKeyId = 65535;
    using keyid_type = uint16_t;

    static const uint8_t sMaxKeyLen = 64;

    // size of the key. 0 indicates it is stored as id
    uint8_t klen() const { return size; }

    // get the key string. Note the string may not be null terminated.
    const char* getKeyStr() const { return key.str_; }

    keyid_type getKeyId() const { return key.id_; }

    unsigned int keyPackedBytes() const {
        return size ? (sizeof(size) + size) : (sizeof(size) + sizeof(keyid_type));
    }

    JsonbValue* value() const { return (JsonbValue*)(((char*)this) + keyPackedBytes()); }

    // size of the total packed bytes (key+value)
    unsigned int numPackedBytes() const;

    uint8_t size;

    union key_ {
        keyid_type id_;
        char str_[1];
    } key;
};

struct JsonbFindResult {
    const JsonbValue* value = nullptr;   // found value
    std::unique_ptr<JsonbWriter> writer; // writer to write the value
    bool is_wildcard = false;            // whether the path is a wildcard path
};

/*
 * JsonbValue is the base class of all JSONB types. It contains only one member
 * variable - type info, which can be retrieved by member functions is[Type]()
 * or type().
 */
struct JsonbValue {
    static const uint32_t sMaxValueLen = 1 << 24; // 16M

    bool isNull() const { return (type == JsonbType::T_Null); }
    bool isTrue() const { return (type == JsonbType::T_True); }
    bool isFalse() const { return (type == JsonbType::T_False); }
    bool isInt() const { return isInt8() || isInt16() || isInt32() || isInt64() || isInt128(); }
    bool isInt8() const { return (type == JsonbType::T_Int8); }
    bool isInt16() const { return (type == JsonbType::T_Int16); }
    bool isInt32() const { return (type == JsonbType::T_Int32); }
    bool isInt64() const { return (type == JsonbType::T_Int64); }
    bool isDouble() const { return (type == JsonbType::T_Double); }
    bool isFloat() const { return (type == JsonbType::T_Float); }
    bool isString() const { return (type == JsonbType::T_String); }
    bool isBinary() const { return (type == JsonbType::T_Binary); }
    bool isObject() const { return (type == JsonbType::T_Object); }
    bool isArray() const { return (type == JsonbType::T_Array); }
    bool isInt128() const { return (type == JsonbType::T_Int128); }
    bool isDecimal() const {
        return (type == JsonbType::T_Decimal32 || type == JsonbType::T_Decimal64 ||
                type == JsonbType::T_Decimal128 || type == JsonbType::T_Decimal256);
    }
    bool isDecimal32() const { return (type == JsonbType::T_Decimal32); }
    bool isDecimal64() const { return (type == JsonbType::T_Decimal64); }
    bool isDecimal128() const { return (type == JsonbType::T_Decimal128); }
    bool isDecimal256() const { return (type == JsonbType::T_Decimal256); }

    PrimitiveType get_primitive_type() const { return get_primitive_type_from_json_type(type); }

    const char* typeName() const {
        switch (type) {
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
        case JsonbType::T_Int128:
            return "largeint";
        case JsonbType::T_Double:
            return "double";
        case JsonbType::T_Float:
            return "float";
        case JsonbType::T_String:
            return "string";
        case JsonbType::T_Binary:
            return "binary";
        case JsonbType::T_Object:
            return "object";
        case JsonbType::T_Array:
            return "array";
        case JsonbType::T_Decimal32:
            return "Decimal32";
        case JsonbType::T_Decimal64:
            return "Decimal64";
        case JsonbType::T_Decimal128:
            return "Decimal128";
        case JsonbType::T_Decimal256:
            return "Decimal256";
        default:
            return "unknown";
        }
    }

    // size of the total packed bytes
    unsigned int numPackedBytes() const;

    // size of the value in bytes
    unsigned int size() const;

    //Get the number of jsonbvalue elements
    int numElements() const;

    //Whether to include the jsonbvalue rhs
    bool contains(JsonbValue* rhs) const;

    // find the JSONB value by JsonbPath
    JsonbFindResult findValue(JsonbPath& path) const;
    friend class JsonbDocument;

    JsonbType type; // type info

    char payload[0]; // payload, which is the packed bytes of the value

    /**
    * @brief Unpacks the underlying Jsonb binary content as a pointer to type `T`.
    *
    * @tparam T A POD (Plain Old Data) type that must satisfy the `JsonbPodType` concept.
    *           This ensures that `T` is trivially copyable, standard-layout, and safe to
    *           reinterpret from raw bytes without invoking undefined behavior.
    *
    * @return A pointer to a `const T` object, interpreted from the internal buffer.
    *
    * @note The caller must ensure that the current JsonbValue actually contains data
    *       compatible with type `T`, otherwise the result is undefined.
    */
    template <JsonbPodType T>
    const T* unpack() const {
        static_assert(is_pod_v<T>, "T must be a POD type");
        return reinterpret_cast<const T*>(payload);
    }

    // /**
    // * @brief Unpacks the underlying Jsonb binary content as a pointer to type `T`.
    // *
    // * @tparam T A POD (Plain Old Data) type that must satisfy the `JsonbPodType` concept.
    // *           This ensures that `T` is trivially copyable, standard-layout, and safe to
    // *           reinterpret from raw bytes without invoking undefined behavior.
    // *
    // * @return A pointer to a `T` object, interpreted from the internal buffer.
    // *
    // * @note The caller must ensure that the current JsonbValue actually contains data
    // *       compatible with type `T`, otherwise the result is undefined.
    // */
    // template <JsonbPodType T>
    // T* unpack() {
    //     static_assert(is_pod_v<T>, "T must be a POD type");
    //     return reinterpret_cast<T*>(payload);
    // }

    int128_t int_val() const;
};

// inline ObjectVal* JsonbDocument::operator->() {
//     return (((JsonbValue*)payload_)->unpack<ObjectVal>());
// }

inline const ObjectVal* JsonbDocument::operator->() const {
    return (((JsonbValue*)payload_)->unpack<ObjectVal>());
}

/*
 * NumerValT is the template class (derived from JsonbValue) of all number
 * types (integers and double).
 */
template <typename T>
    requires std::is_integral_v<T> || std::is_floating_point_v<T>
struct NumberValT {
public:
    T val() const { return num; }

    static unsigned int numPackedBytes() { return sizeof(JsonbValue) + sizeof(T); }

    T num;
};

inline int128_t JsonbValue::int_val() const {
    switch (type) {
    case JsonbType::T_Int8:
        return unpack<JsonbInt8Val>()->val();
    case JsonbType::T_Int16:
        return unpack<JsonbInt16Val>()->val();
    case JsonbType::T_Int32:
        return unpack<JsonbInt32Val>()->val();
    case JsonbType::T_Int64:
        return unpack<JsonbInt64Val>()->val();
    case JsonbType::T_Int128:
        return unpack<JsonbInt128Val>()->val();
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid JSONB value type: {}",
                        static_cast<int32_t>(type));
    }
}

template <JsonbDecimalType T>
struct JsonbDecimalVal {
public:
    using NativeType = typename T::NativeType;

    // get the decimal value
    NativeType val() const {
        // to avoid memory alignment issues, we use memcpy to copy the value
        NativeType tmp;
        memcpy(&tmp, &value, sizeof(NativeType));
        return tmp;
    }

    static constexpr int numPackedBytes() {
        return sizeof(JsonbValue) + sizeof(precision) + sizeof(scale) + sizeof(value);
    }

    uint32_t precision;
    uint32_t scale;
    NativeType value;
};

/*
 * BlobVal is the base class (derived from JsonbValue) for string and binary
 * types. The size indicates the total bytes of the payload.
 */
struct JsonbBinaryVal {
public:
    // size of the blob payload only
    unsigned int getBlobLen() const { return size; }

    // return the blob as byte array
    const char* getBlob() const { return payload; }

    // size of the total packed bytes
    unsigned int numPackedBytes() const { return sizeof(JsonbValue) + sizeof(size) + size; }
    friend class JsonbDocument;

    uint32_t size;
    char payload[0];
};

/*
 * String type
 * Note: JSONB string may not be a c-string (NULL-terminated)
 */
struct JsonbStringVal : public JsonbBinaryVal {
public:
    /*
    This function return the actual size of a string. Since for
    a string, it can be null-terminated with null paddings or it
    can take all the space in the payload without null in the end.
    So we need to check it to get the true actual length of a string.
  */
    size_t length() const {
        // It's an empty string
        if (0 == size) {
            return size;
        }
        // The string stored takes all the spaces in payload
        if (payload[size - 1] != 0) {
            return size;
        }
        // It's shorter than the size of payload
        return strnlen(payload, size);
    }
    // convert the string (case insensitive) to a boolean value
    // "false": 0
    // "true": 1
    // all other strings: -1
    int getBoolVal() {
        if (size == 4 && tolower(payload[0]) == 't' && tolower(payload[1]) == 'r' &&
            tolower(payload[2]) == 'u' && tolower(payload[3]) == 'e') {
            return 1;
        } else if (size == 5 && tolower(payload[0]) == 'f' && tolower(payload[1]) == 'a' &&
                   tolower(payload[2]) == 'l' && tolower(payload[3]) == 's' &&
                   tolower(payload[4]) == 'e') {
            return 0;
        } else {
            return -1;
        }
    }
};

/*
 * ContainerVal is the base class (derived from JsonbValue) for object and
 * array types. The size indicates the total bytes of the payload.
 */
struct ContainerVal {
    // size of the container payload only
    unsigned int getContainerSize() const { return size; }

    // return the container payload as byte array
    const char* getPayload() const { return payload; }

    // size of the total packed bytes
    unsigned int numPackedBytes() const { return sizeof(JsonbValue) + sizeof(size) + size; }
    friend class JsonbDocument;

    uint32_t size;
    char payload[0];
};

/*
 * Object type
 */
struct ObjectVal : public ContainerVal {
    using value_type = JsonbKeyValue;
    using pointer = value_type*;
    using const_pointer = const value_type*;
    using iterator = JsonbFwdIteratorT<pointer, ObjectVal>;
    using const_iterator = JsonbFwdIteratorT<const_pointer, ObjectVal>;

    const_iterator search(const char* key, hDictFind handler = nullptr) const {
        // Calling a non-const method on a const variable and does not modify the
        // variable; using const_cast is permissible
        return const_cast<ObjectVal*>(this)->search(key, handler);
    }

    const_iterator search(const char* key, unsigned int klen, hDictFind handler = nullptr) const {
        // Calling a non-const method on a const variable and does not modify the
        // variable; using const_cast is permissible
        return const_cast<ObjectVal*>(this)->search(key, klen, handler);
    }

    const_iterator search(int key_id) const {
        // Calling a non-const method on a const variable and does not modify the
        // variable; using const_cast is permissible
        return const_cast<ObjectVal*>(this)->search(key_id);
    }
    iterator search(const char* key, hDictFind handler = nullptr) {
        if (!key) {
            return end();
        }
        return search(key, (unsigned int)strlen(key), handler);
    }

    iterator search(const char* key, unsigned int klen, hDictFind handler = nullptr) {
        if (!key || !klen) {
            return end();
        }

        int key_id = -1;
        if (handler && (key_id = handler(key, klen)) >= 0) {
            return search(key_id);
        }
        return internalSearch(key, klen);
    }

    iterator search(int key_id) {
        if (key_id < 0 || key_id > JsonbKeyValue::sMaxKeyId) {
            return end();
        }

        const char* pch = payload;
        const char* fence = payload + size;

        while (pch < fence) {
            auto* pkey = (JsonbKeyValue*)(pch);
            if (!pkey->klen() && key_id == pkey->getKeyId()) {
                return iterator(pkey);
            }
            pch += pkey->numPackedBytes();
        }

        assert(pch == fence);
        return end();
    }

    // Get number of elements in object
    int numElem() const {
        const char* pch = payload;
        const char* fence = payload + size;

        unsigned int num = 0;
        while (pch < fence) {
            auto* pkey = (JsonbKeyValue*)(pch);
            ++num;
            pch += pkey->numPackedBytes();
        }

        assert(pch == fence);

        return num;
    }

    JsonbKeyValue* getJsonbKeyValue(unsigned int i) const {
        const char* pch = payload;
        const char* fence = payload + size;

        unsigned int num = 0;
        while (pch < fence) {
            auto* pkey = (JsonbKeyValue*)(pch);
            if (num == i) {
                return pkey;
            }
            ++num;
            pch += pkey->numPackedBytes();
        }

        assert(pch == fence);

        return nullptr;
    }

    JsonbValue* find(const char* key, hDictFind handler = nullptr) const {
        // Calling a non-const method on a const variable and does not modify the
        // variable; using const_cast is permissible
        return const_cast<ObjectVal*>(this)->find(key, handler);
    }

    JsonbValue* find(const char* key, unsigned int klen, hDictFind handler = nullptr) const {
        // Calling a non-const method on a const variable and does not modify the
        // variable; using const_cast is permissible
        return const_cast<ObjectVal*>(this)->find(key, klen, handler);
    }
    JsonbValue* find(int key_id) const {
        // Calling a non-const method on a const variable and does not modify the
        // variable; using const_cast is permissible
        return const_cast<ObjectVal*>(this)->find(key_id);
    }

    // find the JSONB value by a key string (null terminated)
    JsonbValue* find(const char* key, hDictFind handler = nullptr) {
        if (!key) {
            return nullptr;
        }
        return find(key, (unsigned int)strlen(key), handler);
    }

    // find the JSONB value by a key string (with length)
    JsonbValue* find(const char* key, unsigned int klen, hDictFind handler = nullptr) {
        iterator kv = search(key, klen, handler);
        if (end() == kv) {
            return nullptr;
        }
        return kv->value();
    }

    // find the JSONB value by a key dictionary ID
    JsonbValue* find(int key_id) {
        iterator kv = search(key_id);
        if (end() == kv) {
            return nullptr;
        }
        return kv->value();
    }

    iterator begin() { return iterator((pointer)payload); }

    const_iterator begin() const { return const_iterator((pointer)payload); }

    iterator end() { return iterator((pointer)(payload + size)); }

    const_iterator end() const { return const_iterator((pointer)(payload + size)); }

private:
    iterator internalSearch(const char* key, unsigned int klen) {
        const char* pch = payload;
        const char* fence = payload + size;

        while (pch < fence) {
            auto* pkey = (JsonbKeyValue*)(pch);
            if (klen == pkey->klen() && strncmp(key, pkey->getKeyStr(), klen) == 0) {
                return iterator(pkey);
            }
            pch += pkey->numPackedBytes();
        }

        assert(pch == fence);

        return end();
    }
};

/*
 * Array type
 */
struct ArrayVal : public ContainerVal {
    using value_type = JsonbValue;
    using pointer = value_type*;
    using const_pointer = const value_type*;
    using iterator = JsonbFwdIteratorT<pointer, ArrayVal>;
    using const_iterator = JsonbFwdIteratorT<const_pointer, ArrayVal>;

    // get the JSONB value at index
    JsonbValue* get(int idx) const {
        if (idx < 0) {
            return nullptr;
        }

        const char* pch = payload;
        const char* fence = payload + size;

        while (pch < fence && idx-- > 0) {
            pch += ((JsonbValue*)pch)->numPackedBytes();
        }
        if (idx > 0 || pch == fence) {
            return nullptr;
        }

        return (JsonbValue*)pch;
    }

    // Get number of elements in array
    int numElem() const {
        const char* pch = payload;
        const char* fence = payload + size;

        unsigned int num = 0;
        while (pch < fence) {
            ++num;
            pch += ((JsonbValue*)pch)->numPackedBytes();
        }

        assert(pch == fence);

        return num;
    }

    iterator begin() { return iterator((pointer)payload); }

    const_iterator begin() const { return const_iterator((pointer)payload); }

    iterator end() { return iterator((pointer)(payload + size)); }

    const_iterator end() const { return const_iterator((pointer)(payload + size)); }
};

inline Status JsonbDocument::checkAndCreateDocument(const char* pb, size_t size,
                                                    JsonbDocument** doc) {
    *doc = nullptr;
    if (!pb || size < sizeof(JsonbHeader) + sizeof(JsonbValue)) {
        return Status::InvalidArgument("Invalid JSONB document: too small size({}) or null pointer",
                                       size);
    }

    auto* doc_ptr = (JsonbDocument*)pb;
    if (doc_ptr->header_.ver_ != JSONB_VER) {
        return Status::InvalidArgument("Invalid JSONB document: invalid version({})",
                                       doc_ptr->header_.ver_);
    }

    auto* val = (JsonbValue*)doc_ptr->payload_;
    if (val->type < JsonbType::T_Null || val->type >= JsonbType::NUM_TYPES ||
        size != sizeof(JsonbHeader) + val->numPackedBytes()) {
        return Status::InvalidArgument("Invalid JSONB document: invalid type({}) or size({})",
                                       static_cast<JsonbTypeUnder>(val->type), size);
    }

    *doc = doc_ptr;
    return Status::OK();
}
inline void JsonbDocument::setValue(const JsonbValue* value) {
    memcpy(payload_, value, value->numPackedBytes());
}

inline JsonbValue* JsonbDocument::createValue(const char* pb, size_t size) {
    if (!pb || size < sizeof(JsonbHeader) + sizeof(JsonbValue)) {
        return nullptr;
    }

    auto* doc = (JsonbDocument*)pb;
    if (doc->header_.ver_ != JSONB_VER) {
        return nullptr;
    }

    auto* val = (JsonbValue*)doc->payload_;
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
    auto* val = (JsonbValue*)(((char*)this) + ks);
    return ks + val->numPackedBytes();
}

// Poor man's "virtual" function JsonbValue::numPackedBytes
inline unsigned int JsonbValue::numPackedBytes() const {
    switch (type) {
    case JsonbType::T_Null:
    case JsonbType::T_True:
    case JsonbType::T_False: {
        return sizeof(type);
    }

    case JsonbType::T_Int8: {
        return sizeof(type) + sizeof(int8_t);
    }
    case JsonbType::T_Int16: {
        return sizeof(type) + sizeof(int16_t);
    }
    case JsonbType::T_Int32: {
        return sizeof(type) + sizeof(int32_t);
    }
    case JsonbType::T_Int64: {
        return sizeof(type) + sizeof(int64_t);
    }
    case JsonbType::T_Double: {
        return sizeof(type) + sizeof(double);
    }
    case JsonbType::T_Float: {
        return sizeof(type) + sizeof(float);
    }
    case JsonbType::T_Int128: {
        return sizeof(type) + sizeof(int128_t);
    }
    case JsonbType::T_String:
    case JsonbType::T_Binary: {
        return unpack<JsonbBinaryVal>()->numPackedBytes();
    }

    case JsonbType::T_Object:
    case JsonbType::T_Array: {
        return unpack<ContainerVal>()->numPackedBytes();
    }
    case JsonbType::T_Decimal32: {
        return JsonbDecimal32::numPackedBytes();
    }
    case JsonbType::T_Decimal64: {
        return JsonbDecimal64::numPackedBytes();
    }
    case JsonbType::T_Decimal128: {
        return JsonbDecimal128::numPackedBytes();
    }
    case JsonbType::T_Decimal256: {
        return JsonbDecimal256::numPackedBytes();
    }
    case JsonbType::NUM_TYPES:
        break;
    }

    throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid JSONB value type: {}",
                    static_cast<int32_t>(type));
}

inline int JsonbValue::numElements() const {
    switch (type) {
    case JsonbType::T_Int8:
    case JsonbType::T_Int16:
    case JsonbType::T_Int32:
    case JsonbType::T_Int64:
    case JsonbType::T_Double:
    case JsonbType::T_Float:
    case JsonbType::T_Int128:
    case JsonbType::T_String:
    case JsonbType::T_Binary:
    case JsonbType::T_Null:
    case JsonbType::T_True:
    case JsonbType::T_False:
    case JsonbType::T_Decimal32:
    case JsonbType::T_Decimal64:
    case JsonbType::T_Decimal128:
    case JsonbType::T_Decimal256: {
        return 1;
    }
    case JsonbType::T_Object: {
        return unpack<ObjectVal>()->numElem();
    }
    case JsonbType::T_Array: {
        return unpack<ArrayVal>()->numElem();
    }
    case JsonbType::NUM_TYPES:
        break;
    }
    throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid JSONB value type: {}",
                    static_cast<int32_t>(type));
}

inline bool JsonbValue::contains(JsonbValue* rhs) const {
    switch (type) {
    case JsonbType::T_Int8:
    case JsonbType::T_Int16:
    case JsonbType::T_Int32:
    case JsonbType::T_Int64:
    case JsonbType::T_Int128: {
        return rhs->isInt() && this->int_val() == rhs->int_val();
    }
    case JsonbType::T_Double:
    case JsonbType::T_Float: {
        if (!rhs->isDouble() && !rhs->isFloat()) {
            return false;
        }
        double left = isDouble() ? unpack<JsonbDoubleVal>()->val() : unpack<JsonbFloatVal>()->val();
        double right = rhs->isDouble() ? rhs->unpack<JsonbDoubleVal>()->val()
                                       : rhs->unpack<JsonbFloatVal>()->val();
        return left == right;
    }
    case JsonbType::T_String:
    case JsonbType::T_Binary: {
        if (rhs->isString() || rhs->isBinary()) {
            const auto* str_value1 = unpack<JsonbStringVal>();
            const auto* str_value2 = rhs->unpack<JsonbStringVal>();
            return str_value1->length() == str_value2->length() &&
                   std::memcmp(str_value1->getBlob(), str_value2->getBlob(),
                               str_value1->length()) == 0;
        }
        return false;
    }
    case JsonbType::T_Array: {
        int lhs_num = unpack<ArrayVal>()->numElem();
        if (rhs->isArray()) {
            int rhs_num = rhs->unpack<ArrayVal>()->numElem();
            if (rhs_num > lhs_num) {
                return false;
            }
            int contains_num = 0;
            for (int i = 0; i < lhs_num; ++i) {
                for (int j = 0; j < rhs_num; ++j) {
                    if (unpack<ArrayVal>()->get(i)->contains(rhs->unpack<ArrayVal>()->get(j))) {
                        contains_num++;
                        break;
                    }
                }
            }
            return contains_num == rhs_num;
        }
        for (int i = 0; i < lhs_num; ++i) {
            if (unpack<ArrayVal>()->get(i)->contains(rhs)) {
                return true;
            }
        }
        return false;
    }
    case JsonbType::T_Object: {
        if (rhs->isObject()) {
            const auto* obj_value1 = unpack<ObjectVal>();
            const auto* obj_value2 = rhs->unpack<ObjectVal>();
            for (int i = 0; i < obj_value2->numElem(); ++i) {
                JsonbKeyValue* key = obj_value2->getJsonbKeyValue(i);
                JsonbValue* value = obj_value1->find(key->getKeyStr(), key->klen());
                if (value == nullptr || !value->contains(key->value())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    case JsonbType::T_Null: {
        return rhs->isNull();
    }
    case JsonbType::T_True: {
        return rhs->isTrue();
    }
    case JsonbType::T_False: {
        return rhs->isFalse();
    }
    case JsonbType::T_Decimal32: {
        if (rhs->isDecimal32()) {
            return unpack<JsonbDecimal32>()->val() == rhs->unpack<JsonbDecimal32>()->val() &&
                   unpack<JsonbDecimal32>()->precision ==
                           rhs->unpack<JsonbDecimal32>()->precision &&
                   unpack<JsonbDecimal32>()->scale == rhs->unpack<JsonbDecimal32>()->scale;
        }
        return false;
    }
    case JsonbType::T_Decimal64: {
        if (rhs->isDecimal64()) {
            return unpack<JsonbDecimal64>()->val() == rhs->unpack<JsonbDecimal64>()->val() &&
                   unpack<JsonbDecimal64>()->precision ==
                           rhs->unpack<JsonbDecimal64>()->precision &&
                   unpack<JsonbDecimal64>()->scale == rhs->unpack<JsonbDecimal64>()->scale;
        }
        return false;
    }
    case JsonbType::T_Decimal128: {
        if (rhs->isDecimal128()) {
            return unpack<JsonbDecimal128>()->val() == rhs->unpack<JsonbDecimal128>()->val() &&
                   unpack<JsonbDecimal128>()->precision ==
                           rhs->unpack<JsonbDecimal128>()->precision &&
                   unpack<JsonbDecimal128>()->scale == rhs->unpack<JsonbDecimal128>()->scale;
        }
        return false;
    }
    case JsonbType::T_Decimal256: {
        if (rhs->isDecimal256()) {
            return unpack<JsonbDecimal256>()->val() == rhs->unpack<JsonbDecimal256>()->val() &&
                   unpack<JsonbDecimal256>()->precision ==
                           rhs->unpack<JsonbDecimal256>()->precision &&
                   unpack<JsonbDecimal256>()->scale == rhs->unpack<JsonbDecimal256>()->scale;
        }
        return false;
    }
    case JsonbType::NUM_TYPES:
        break;
    }

    throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid JSONB value type: {}",
                    static_cast<int32_t>(type));
}

inline bool JsonbPath::seek(const char* key_path, size_t kp_len) {
    while (kp_len > 0 && std::isspace(key_path[kp_len - 1])) {
        --kp_len;
    }

    //path invalid
    if (!key_path || kp_len == 0) {
        return false;
    }
    Stream stream(key_path, kp_len);
    stream.skip_whitespace();
    if (stream.exhausted() || stream.read() != SCOPE) {
        //path invalid
        return false;
    }

    while (!stream.exhausted()) {
        stream.skip_whitespace();
        stream.clear_leg_ptr();
        stream.clear_leg_len();

        if (!JsonbPath::parsePath(&stream, this)) {
            //path invalid
            return false;
        }
    }
    return true;
}

inline bool JsonbPath::parsePath(Stream* stream, JsonbPath* path) {
    // $[0]
    if (stream->peek() == BEGIN_ARRAY) {
        return parse_array(stream, path);
    }
    // $.a or $.[0]
    else if (stream->peek() == BEGIN_MEMBER) {
        // advance past the .
        stream->skip(1);

        if (stream->exhausted()) {
            return false;
        }

        // $.[0]
        if (stream->peek() == BEGIN_ARRAY) {
            return parse_array(stream, path);
        }
        // $.a
        else {
            return parse_member(stream, path);
        }
    } else if (stream->peek() == WILDCARD) {
        stream->skip(1);
        if (stream->exhausted()) {
            return false;
        }

        // $**
        if (stream->peek() == WILDCARD) {
            path->_is_supper_wildcard = true;
        }

        stream->skip(1);
        if (stream->exhausted()) {
            return false;
        }

        if (stream->peek() == BEGIN_ARRAY) {
            return parse_array(stream, path);
        } else if (stream->peek() == BEGIN_MEMBER) {
            // advance past the .
            stream->skip(1);

            if (stream->exhausted()) {
                return false;
            }

            // $.[0]
            if (stream->peek() == BEGIN_ARRAY) {
                return parse_array(stream, path);
            }
            // $.a
            else {
                return parse_member(stream, path);
            }
        }
        return false;
    } else {
        return false; //invalid json path
    }
}

inline bool JsonbPath::parse_array(Stream* stream, JsonbPath* path) {
    assert(stream->peek() == BEGIN_ARRAY);
    stream->skip(1);
    if (stream->exhausted()) {
        return false;
    }

    if (stream->peek() == WILDCARD) {
        // Called by function_jsonb.cpp, the variables passed in originate from a mutable block;
        // using const_cast is acceptable.
        stream->set_leg_ptr(const_cast<char*>(stream->position()));
        stream->add_leg_len();
        stream->skip(1);
        if (stream->exhausted()) {
            return false;
        }

        if (stream->peek() == END_ARRAY) {
            std::unique_ptr<leg_info> leg(
                    new leg_info(stream->get_leg_ptr(), stream->get_leg_len(), 0, ARRAY_CODE));
            path->add_leg_to_leg_vector(std::move(leg));
            stream->skip(1);
            path->_is_wildcard = true;
            return true;
        } else {
            return false;
        }
    }

    // Called by function_jsonb.cpp, the variables passed in originate from a mutable block;
    // using const_cast is acceptable.
    stream->set_leg_ptr(const_cast<char*>(stream->position()));

    for (; !stream->exhausted() && stream->peek() != END_ARRAY; stream->advance()) {
        stream->add_leg_len();
    }

    if (stream->exhausted() || stream->peek() != END_ARRAY) {
        return false;
    } else {
        stream->skip(1);
    }

    //parse array index to int

    std::string_view idx_string(stream->get_leg_ptr(), stream->get_leg_len());
    int index = 0;

    if (stream->get_leg_len() >= 4 &&
        std::equal(LAST, LAST + 4, stream->get_leg_ptr(),
                   [](char c1, char c2) { return std::tolower(c1) == std::tolower(c2); })) {
        auto pos = idx_string.find(MINUS);

        if (pos != std::string::npos) {
            for (size_t i = 4; i < pos; ++i) {
                if (std::isspace(idx_string[i])) {
                    continue;
                } else {
                    // leading zeroes are not allowed
                    LOG(WARNING) << "Non-space char in idx_string: '" << idx_string << "'";
                    return false;
                }
            }
            idx_string = idx_string.substr(pos + 1);
            idx_string = trim(idx_string);

            auto result = std::from_chars(idx_string.data(), idx_string.data() + idx_string.size(),
                                          index);
            if (result.ec != std::errc()) {
                LOG(WARNING) << "Invalid index in JSON path: '" << idx_string << "'";
                return false;
            }

        } else if (stream->get_leg_len() > 4) {
            return false;
        }

        std::unique_ptr<leg_info> leg(new leg_info(nullptr, 0, -index - 1, ARRAY_CODE));
        path->add_leg_to_leg_vector(std::move(leg));

        return true;
    }

    auto result = std::from_chars(idx_string.data(), idx_string.data() + idx_string.size(), index);

    if (result.ec != std::errc()) {
        return false;
    }

    std::unique_ptr<leg_info> leg(new leg_info(nullptr, 0, index, ARRAY_CODE));
    path->add_leg_to_leg_vector(std::move(leg));

    return true;
}

inline bool JsonbPath::parse_member(Stream* stream, JsonbPath* path) {
    if (stream->exhausted()) {
        return false;
    }

    if (stream->peek() == WILDCARD) {
        // Called by function_jsonb.cpp, the variables passed in originate from a mutable block;
        // using const_cast is acceptable.
        stream->set_leg_ptr(const_cast<char*>(stream->position()));
        stream->add_leg_len();
        stream->skip(1);
        std::unique_ptr<leg_info> leg(
                new leg_info(stream->get_leg_ptr(), stream->get_leg_len(), 0, MEMBER_CODE));
        path->add_leg_to_leg_vector(std::move(leg));
        path->_is_wildcard = true;
        return true;
    }

    // Called by function_jsonb.cpp, the variables passed in originate from a mutable block;
    // using const_cast is acceptable.
    stream->set_leg_ptr(const_cast<char*>(stream->position()));

    const char* left_quotation_marks = nullptr;
    const char* right_quotation_marks = nullptr;

    for (; !stream->exhausted(); stream->advance()) {
        // Only accept space characters quoted by double quotes.
        if (std::isspace(stream->peek()) && left_quotation_marks == nullptr) {
            return false;
        } else if (stream->peek() == ESCAPE) {
            stream->add_leg_len();
            stream->skip(1);
            stream->add_leg_len();
            stream->set_has_escapes(true);
            if (stream->exhausted()) {
                return false;
            }
            continue;
        } else if (stream->peek() == DOUBLE_QUOTE) {
            if (left_quotation_marks == nullptr) {
                left_quotation_marks = stream->position();
                // Called by function_jsonb.cpp, the variables passed in originate from a mutable block;
                // using const_cast is acceptable.
                stream->set_leg_ptr(const_cast<char*>(++left_quotation_marks));
                continue;
            } else {
                right_quotation_marks = stream->position();
                stream->skip(1);
                break;
            }
        } else if (stream->peek() == BEGIN_MEMBER || stream->peek() == BEGIN_ARRAY) {
            if (left_quotation_marks == nullptr) {
                break;
            }
        }

        stream->add_leg_len();
    }

    if ((left_quotation_marks != nullptr && right_quotation_marks == nullptr) ||
        stream->get_leg_ptr() == nullptr || stream->get_leg_len() == 0) {
        return false; //invalid json path
    }

    if (stream->get_has_escapes()) {
        stream->remove_escapes();
    }

    std::unique_ptr<leg_info> leg(
            new leg_info(stream->get_leg_ptr(), stream->get_leg_len(), 0, MEMBER_CODE));
    path->add_leg_to_leg_vector(std::move(leg));

    return true;
}

static_assert(is_pod_v<JsonbDocument>, "JsonbDocument must be standard layout and trivial");
static_assert(is_pod_v<JsonbValue>, "JsonbValue must be standard layout and trivial");
static_assert(is_pod_v<JsonbDecimal32>, "JsonbDecimal32 must be standard layout and trivial");
static_assert(is_pod_v<JsonbDecimal64>, "JsonbDecimal64 must be standard layout and trivial");
static_assert(is_pod_v<JsonbDecimal128>, "JsonbDecimal128 must be standard layout and trivial");
static_assert(is_pod_v<JsonbDecimal256>, "JsonbDecimal256 must be standard layout and trivial");
static_assert(is_pod_v<JsonbInt8Val>, "JsonbInt8Val must be standard layout and trivial");
static_assert(is_pod_v<JsonbInt32Val>, "JsonbInt32Val must be standard layout and trivial");
static_assert(is_pod_v<JsonbInt64Val>, "JsonbInt64Val must be standard layout and trivial");
static_assert(is_pod_v<JsonbInt128Val>, "JsonbInt128Val must be standard layout and trivial");
static_assert(is_pod_v<JsonbDoubleVal>, "JsonbDoubleVal must be standard layout and trivial");
static_assert(is_pod_v<JsonbFloatVal>, "JsonbFloatVal must be standard layout and trivial");
static_assert(is_pod_v<JsonbBinaryVal>, "JsonbBinaryVal must be standard layout and trivial");
static_assert(is_pod_v<ContainerVal>, "ContainerVal must be standard layout and trivial");

#define ASSERT_DECIMAL_LAYOUT(type)                \
    static_assert(offsetof(type, precision) == 0); \
    static_assert(offsetof(type, scale) == 4);     \
    static_assert(offsetof(type, value) == 8);

ASSERT_DECIMAL_LAYOUT(JsonbDecimal32)
ASSERT_DECIMAL_LAYOUT(JsonbDecimal64)
ASSERT_DECIMAL_LAYOUT(JsonbDecimal128)
ASSERT_DECIMAL_LAYOUT(JsonbDecimal256)

#define ASSERT_NUMERIC_LAYOUT(type) static_assert(offsetof(type, num) == 0);

ASSERT_NUMERIC_LAYOUT(JsonbInt8Val)
ASSERT_NUMERIC_LAYOUT(JsonbInt32Val)
ASSERT_NUMERIC_LAYOUT(JsonbInt64Val)
ASSERT_NUMERIC_LAYOUT(JsonbInt128Val)
ASSERT_NUMERIC_LAYOUT(JsonbDoubleVal)

static_assert(offsetof(JsonbBinaryVal, size) == 0);
static_assert(offsetof(JsonbBinaryVal, payload) == 4);

static_assert(offsetof(ContainerVal, size) == 0);
static_assert(offsetof(ContainerVal, payload) == 4);

#pragma pack(pop)
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
} // namespace doris

#endif // JSONB_JSONBDOCUMENT_H
