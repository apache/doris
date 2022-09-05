#pragma once

#include <common/status.h>
#include <vec/columns/column.h>
#include <vec/json/json_parser.h>
#include <vec/json/simd_json_parser.h>

namespace doris::vectorized {

// parse a batch of json strings into column object
Status parse_json_to_variant(IColumn& column, const std::vector<StringRef>& jsons);

// parse a single json
Status parse_json_to_variant(IColumn& column, const StringRef& jsons, JSONDataParser<SimdJSONParser>* parser);

// extract keys columns from json strings into columns
bool extract_key(MutableColumns& columns, const std::vector<StringRef>& jsons,
                const std::vector<StringRef>& keys, const std::vector<ExtractType>& types);

// extract keys columns from colunnstring(json format) into columns
bool extract_key(MutableColumns& columns, const ColumnString& json_column,
                const std::vector<StringRef>& keys, const std::vector<ExtractType>& types);
}
