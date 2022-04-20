#include "field_visitors.h"

#include <sstream>

#include "runtime/large_int_value.h"


namespace doris::vectorized {

inline void writeQuoted(const std::string& x, std::ostringstream& buf) {
    buf << '\'';
    buf << x;
    buf << '\'';
}

template <typename T>
static inline void writeQuoted(const T& x, std::ostringstream& buf) {
    buf << '\'';
    buf << std::to_string(x);
    buf << '\'';
}

template <typename T>
static inline void writeQuoted(const DecimalField<T>& x, std::ostringstream& buf) {
    buf << '\'';
    buf << std::to_string(x.get_value());
    buf << '\'';
}

template<>
inline void writeQuoted(const DecimalField<Decimal128>& x, std::ostringstream& buf) {
    buf << '\'';
    buf << DecimalV2Value(x.get_value()).to_string();
    buf << '\'';
}

template<>
inline void writeQuoted(const UInt128& x, std::ostringstream& buf) {
    buf << '\'';
    buf << x.to_hex_string();
    buf << '\'';
}

template <typename T>
static inline std::string formatQuoted(T x) {
    std::ostringstream buffer;
    writeQuoted(x, buffer);
    return buffer.str();
}

String FieldVisitorToString::operator()(const Null& x) const { return "NULL"; }
String FieldVisitorToString::operator()(const UInt64& x) const { return formatQuoted(x); }
String FieldVisitorToString::operator()(const UInt128& x) const { return formatQuoted(x); }
String FieldVisitorToString::operator()(const Int64& x) const { return formatQuoted(x); }
String FieldVisitorToString::operator()(const Float64& x) const { return formatQuoted(x); }
String FieldVisitorToString::operator()(const String& x) const { return formatQuoted(x); }
String FieldVisitorToString::operator()(const Array& x) const { __builtin_unreachable(); }
String FieldVisitorToString::operator()(const Tuple& x) const { __builtin_unreachable(); }
String FieldVisitorToString::operator()(const DecimalField<Decimal32>& x) const { return formatQuoted(x); }
String FieldVisitorToString::operator()(const DecimalField<Decimal64>& x) const { return formatQuoted(x); }
String FieldVisitorToString::operator()(const DecimalField<Decimal128>& x) const { return formatQuoted(x); }
String FieldVisitorToString::operator()(const AggregateFunctionStateData& x) const { return formatQuoted(x.data); }

}