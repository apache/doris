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

#include "kml_parse.h"

#include <libxml/xmlstring.h>

#include "kml_parse_ctx.h"

namespace doris {

xmlSAXHandler KmlParser::sax_handler = {.startElement = start_element,
                                        .endElement = end_element,
                                        .characters = characters,
                                        .initialized = XML_SAX2_MAGIC};

GeoParseStatus KmlParser::parse(const std::string& kml_input, KmlParseContext& ctx) {
    // Create a push parser context with the SAX handler and the provided context
    xmlParserCtxtPtr ctxt = xmlCreatePushParserCtxt(&sax_handler, &ctx, nullptr, 0, nullptr);
    if (!ctxt) {
        return GEO_PARSE_XML_SYNTAX_ERROR;
    }

    // Parse the KML input data in chunks (here, the entire data is pushed at once)
    xmlParseChunk(ctxt, kml_input.data(), kml_input.size(), 0);

    // Check if the context status indicates an error during parsing
    if (ctx.status != GEO_PARSE_OK) {
        // Stop the parser if an error is encountered
        xmlStopParser(ctxt);
    }

    // Finalize the parsing process by signaling the end of the input
    xmlParseChunk(ctxt, nullptr, 0, 1);

    // Check if the XML is well-formed
    if (ctxt->wellFormed != 1) {
        // Free the parser context and return a syntax error if the XML is not well-formed
        xmlFreeParserCtxt(ctxt);
        return GEO_PARSE_XML_SYNTAX_ERROR;
    }

    // Free the parser context
    xmlFreeParserCtxt(ctxt);

    // Return the final status recorded in the context
    return ctx.status;
}

void KmlParser::start_element(void* ctx, const xmlChar* name, const xmlChar** attrs) {
    auto* context = static_cast<KmlParseContext*>(ctx);
    if (context->status != GEO_PARSE_OK) {
        return;
    }

    const char* tag = reinterpret_cast<const char*>(name);
    if (strcasecmp(tag, "Point") == 0) {
        context->reset();
        context->current_shape_type = GEO_SHAPE_POINT;
    } else if (strcasecmp(tag, "LineString") == 0) {
        context->reset();
        context->current_shape_type = GEO_SHAPE_LINE_STRING;
    } else if (strcasecmp(tag, "Polygon") == 0) {
        context->reset();
        context->current_shape_type = GEO_SHAPE_POLYGON;
    } else if (strcasecmp(tag, "outerBoundaryIs") == 0) {
        context->polygon_ctx.in_outer_boundary = true;
    } else if (strcasecmp(tag, "innerBoundaryIs") == 0) {
        context->polygon_ctx.in_inner_boundary = true;
        if (context->coordinates == nullptr) {
            context->coordinates = new GeoCoordinateList();
        }
    } else if (strcasecmp(tag, "coordinates") == 0) {
        context->in_coordinates = true;
        context->char_buffer.clear();
    }
}

void KmlParser::end_element(void* ctx, const xmlChar* name) {
    auto* context = static_cast<KmlParseContext*>(ctx);
    if (context->status != GEO_PARSE_OK) {
        return;
    }

    const char* tag = reinterpret_cast<const char*>(name);
    if (strcasecmp(tag, "coordinates") == 0) {
        context->in_coordinates = false;
        // Parse the coordinate data into the current geometry type
        bool success = context->parse_coordinates(context->char_buffer);
        if (success) {
            if (context->current_shape_type == GEO_SHAPE_POLYGON) {
                // First ring is the outer ring
                if (context->polygon_ctx.in_outer_boundary) {
                    context->polygon_ctx.rings.add(context->coordinates);
                    context->coordinates = nullptr;
                } else {
                    context->status = GEO_PARSE_POLYGON_SYNTAX_ERROR;
                }
            }
        }
    } else if (strcasecmp(tag, "Point") == 0 || strcasecmp(tag, "LineString") == 0 ||
               strcasecmp(tag, "Polygon") == 0) {
        // Geometric object parsing completed, generating GepShape
        context->build_shape();
    }
}

void KmlParser::characters(void* ctx, const xmlChar* ch, int len) {
    auto* context = static_cast<KmlParseContext*>(ctx);
    if (context->status != GEO_PARSE_OK) {
        return;
    }
    if (context->in_coordinates) {
        context->char_buffer.append(reinterpret_cast<const char*>(ch), len);
    }
}

} // namespace doris
