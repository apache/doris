// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <sstream>

#include "rapidjson/document.h"

// Routines for rendering Mustache (http://mustache.github.io) templates with RapidJson
// (https://code.google.com/p/rapidjson/) documents.
namespace mustache {

// Render a template contained in 'document' with respect to the json context
// 'context'. Alternately finds a tag and then evaluates it. Returns when an error is
// signalled (TODO: probably doesn't work in all paths), and evaluates that tag. Output is
// accumulated in 'out'.
bool RenderTemplate(const std::string& document, const std::string& document_root,
                    const rapidjson::Value& context, std::stringstream* out);

} // namespace mustache
