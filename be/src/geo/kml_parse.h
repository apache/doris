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

#include <libxml/parser.h>

#include "geo/geo_common.h"
#include "geo/kml_parse_ctx.h"

namespace doris {

class KmlParser {
public:
    // Parse KML to a GeoShape
    // Return a valid GeoShape if input KML is supported
    // Return null if KML is not supported or invalid
    static GeoParseStatus parse(const std::string& kml_input, KmlParseContext& ctx);

private:
    // Callback function triggered when an XML start tag is encountered
    static void start_element(void* ctx, const xmlChar* name, const xmlChar** attrs);
    // Callback function triggered when an XML end tag is encountered
    static void end_element(void* ctx, const xmlChar* name);
    // Callback function triggered when text content inside an XML element is encountered
    static void characters(void* ctx, const xmlChar* ch, int len);

    // Declare the SAX handler structure
    static xmlSAXHandler sax_handler;
};

} // namespace doris
