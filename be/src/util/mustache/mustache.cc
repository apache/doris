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

#include "mustache.h"

#include <rapidjson/prettywriter.h>

#include <boost/algorithm/string.hpp>
#include <fstream>
#include <iostream>
#include <stack>
#include <vector>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

using namespace rapidjson;
using namespace std;
using namespace boost::algorithm;

namespace mustache {

// TODO:
// # Handle malformed templates better
// # Better support for reading templates from files

enum TagOperator {
    SUBSTITUTION,
    SECTION_START,
    NEGATED_SECTION_START,
    PREDICATE_SECTION_START,
    SECTION_END,
    PARTIAL,
    COMMENT,
    LENGTH,
    EQUALITY,
    INEQUALITY,
    LITERAL,
    NONE
};

struct OpCtx {
    TagOperator op;
    string tag_name;
    string tag_arg;
    bool escaped = false;
};

struct ContextStack {
    const Value* value;
    const ContextStack* parent;
};

TagOperator GetOperator(const string& tag) {
    if (tag.size() == 0) return SUBSTITUTION;
    switch (tag[0]) {
    case '#':
        return SECTION_START;
    case '^':
        return NEGATED_SECTION_START;
    case '?':
        return PREDICATE_SECTION_START;
    case '/':
        return SECTION_END;
    case '>':
        return PARTIAL;
    case '!':
        if (tag.size() == 1 || tag[1] != '=') return COMMENT;
        return INEQUALITY;
    case '%':
        return LENGTH;
    case '~':
        return LITERAL;
    case '=':
        return EQUALITY;
    default:
        return SUBSTITUTION;
    }
}

int EvaluateTag(const string& document, const string& document_root, int idx,
                const ContextStack* context, const OpCtx& op_ctx, stringstream* out);

static bool RenderTemplate(const string& document, const string& document_root,
                           const ContextStack* stack, stringstream* out);

void EscapeHtml(const string& in, stringstream* out) {
    for (const char& c : in) {
        switch (c) {
        case '&':
            (*out) << "&amp;";
            break;
        case '"':
            (*out) << "&quot;";
            break;
        case '\'':
            (*out) << "&apos;";
            break;
        case '<':
            (*out) << "&lt;";
            break;
        case '>':
            (*out) << "&gt;";
            break;
        default:
            (*out) << c;
            break;
        }
    }
}

void Dump(const rapidjson::Value& v) {
    StringBuffer buffer;
    Writer<StringBuffer> writer(buffer);
    v.Accept(writer);
    std::cout << buffer.GetString() << std::endl;
}

// Breaks a dotted path into individual components. One wrinkle, which stops this from
// being a simple split() is that we allow path components to be quoted, e.g.: "foo".bar,
// and any '.' characters inside those quoted sections aren't considered to be
// delimiters. This is to allow Json keys that contain periods.
void FindJsonPathComponents(const string& path, vector<string>* components) {
    bool in_quote = false;
    bool escape_this_char = false;
    int start = 0;
    for (int i = start; i < path.size(); ++i) {
        if (path[i] == '"' && !escape_this_char) in_quote = !in_quote;
        if (path[i] == '.' && !escape_this_char && !in_quote) {
            // Current char == delimiter and not escaped and not in a quote pair => found a
            // component
            if (i - start > 0) {
                if (path[start] == '"' && path[(i - 1) - start] == '"') {
                    if (i - start > 3) {
                        components->push_back(path.substr(start + 1, i - (start + 2)));
                    }
                } else {
                    components->push_back(path.substr(start, i - start));
                }
                start = i + 1;
            }
        }

        escape_this_char = (path[i] == '\\' && !escape_this_char);
    }

    if (path.size() - start > 0) {
        if (path[start] == '"' && path[(path.size() - 1) - start] == '"') {
            if (path.size() - start > 3) {
                components->push_back(path.substr(start + 1, path.size() - (start + 2)));
            }
        } else {
            components->push_back(path.substr(start, path.size() - start));
        }
    }
}

// Looks up the json entity at 'path' in 'parent_context', and places it in 'resolved'. If
// the entity does not exist (i.e. the path is invalid), 'resolved' will be set to nullptr.
void ResolveJsonContext(const string& path, const ContextStack* stack, const Value** resolved) {
    if (path == ".") {
        *resolved = stack->value;
        return;
    }
    vector<string> components;
    FindJsonPathComponents(path, &components);

    // At each enclosing level of context, try to resolve the path.
    for (; stack != nullptr; stack = stack->parent) {
        const Value* cur = stack->value;
        bool match = true;
        for (const string& c : components) {
            if (cur->IsObject() && cur->HasMember(c.c_str())) {
                cur = &(*cur)[c.c_str()];
            } else {
                match = false;
                break;
            }
        }
        if (match) {
            *resolved = cur;
            return;
        }
    }
    *resolved = nullptr;
}

int FindNextTag(const string& document, int idx, OpCtx* op, stringstream* out) {
    op->op = NONE;
    while (idx < document.size()) {
        if (document[idx] == '{' && idx < (document.size() - 3) && document[idx + 1] == '{') {
            if (document[idx + 2] == '{') {
                idx += 3;
                op->escaped = true;
            } else {
                op->escaped = false;
                idx += 2; // Now at start of template expression
            }
            stringstream expr;
            while (idx < document.size()) {
                if (document[idx] != '}') {
                    expr << document[idx];
                    ++idx;
                } else {
                    if (!op->escaped && idx < document.size() - 1 && document[idx + 1] == '}') {
                        ++idx;
                        break;
                    } else if (op->escaped && idx < document.size() - 2 &&
                               document[idx + 1] == '}' && document[idx + 2] == '}') {
                        idx += 2;
                        break;
                    } else {
                        expr << '}';
                    }
                }
            }

            string key = expr.str();
            trim(key);
            if (key != ".") trim_if(key, is_any_of("."));
            if (key.size() == 0) continue;
            op->op = GetOperator(key);
            if (op->op != SUBSTITUTION) {
                int len = op->op == INEQUALITY ? 2 : 1;
                key = key.substr(len);
                trim(key);
            }
            if (key.size() == 0) continue;

            if (op->op == EQUALITY || op->op == INEQUALITY) {
                // Find an argument
                vector<string> components;
                split(components, key, is_any_of(" "));
                key = components[0];
                components.erase(components.begin());
                op->tag_arg = join(components, " ");
            }

            op->tag_name = key;
            return ++idx;
        } else {
            if (out != nullptr) (*out) << document[idx];
        }
        ++idx;
    }
    return idx;
}

// Evaluates a [PREDICATE_|NEGATED_]SECTION_START / SECTION_END pair by evaluating the tag
// in 'parent_context'. False or non-existant values cause the entire section to be
// skipped. True values cause the section to be evaluated as though it were a normal
// section, but with the parent context being the root context for that section.
//
// If 'is_negation' is true, the behaviour is the opposite of the above: false values
// cause the section to be normally evaluated etc.
int EvaluateSection(const string& document, const string& document_root, int idx,
                    const ContextStack* context_stack, const OpCtx& op_ctx, stringstream* out) {
    // Precondition: idx is the immediate next character after an opening {{ #tag_name }}
    const Value* context;
    ResolveJsonContext(op_ctx.tag_name, context_stack, &context);

    // If we a) cannot resolve the context from the tag name or b) the context evaluates to
    // false, we should skip the contents of the template until a closing {{/tag_name}}.
    bool skip_contents = false;

    if (op_ctx.op == NEGATED_SECTION_START || op_ctx.op == PREDICATE_SECTION_START ||
        op_ctx.op == SECTION_START) {
        skip_contents = (context == nullptr || context->IsFalse());

        // If the tag is a negative block (i.e. {{^tag_name}}), do the opposite: if the
        // context exists and is true, skip the contents, else echo them.
        if (op_ctx.op == NEGATED_SECTION_START) {
            context = context_stack->value;
            skip_contents = !skip_contents;
        } else if (op_ctx.op == PREDICATE_SECTION_START) {
            context = context_stack->value;
        }
    } else if (op_ctx.op == INEQUALITY || op_ctx.op == EQUALITY) {
        skip_contents = (context == nullptr || !context->IsString() ||
                         strcasecmp(context->GetString(), op_ctx.tag_arg.c_str()) != 0);
        if (op_ctx.op == INEQUALITY) skip_contents = !skip_contents;
        context = context_stack->value;
    }

    vector<const Value*> values;
    if (!skip_contents && context != nullptr && context->IsArray()) {
        for (int i = 0; i < context->Size(); ++i) {
            values.push_back(&(*context)[i]);
        }
    } else {
        values.push_back(skip_contents ? nullptr : context);
    }
    if (values.size() == 0) {
        skip_contents = true;
        values.push_back(nullptr);
    }

    int start_idx = idx;
    for (const Value* v : values) {
        idx = start_idx;
        stack<OpCtx> section_starts;
        section_starts.push(op_ctx);
        while (idx < document.size()) {
            OpCtx next_ctx;
            idx = FindNextTag(document, idx, &next_ctx, skip_contents ? nullptr : out);
            if (skip_contents &&
                (next_ctx.op == SECTION_START || next_ctx.op == PREDICATE_SECTION_START ||
                 next_ctx.op == NEGATED_SECTION_START)) {
                section_starts.push(next_ctx);
            } else if (next_ctx.op == SECTION_END) {
                if (next_ctx.tag_name != section_starts.top().tag_name) return -1;
                section_starts.pop();
            }
            if (section_starts.empty()) break;

            // Don't need to evaluate any templates if we're skipping the contents
            if (!skip_contents) {
                ContextStack new_context = {v, context_stack};
                idx = EvaluateTag(document, document_root, idx, &new_context, next_ctx, out);
            }
        }
    }
    return idx;
}

// Evaluates a SUBSTITUTION tag, by replacing its contents with the value of the tag's
// name in 'parent_context'.
int EvaluateSubstitution(const string& document, const int idx, const ContextStack* context_stack,
                         const OpCtx& op_ctx, stringstream* out) {
    const Value* val;
    ResolveJsonContext(op_ctx.tag_name, context_stack, &val);
    if (val == nullptr) return idx;
    if (val->IsString()) {
        if (!op_ctx.escaped) {
            EscapeHtml(val->GetString(), out);
        } else {
            // TODO: Triple {{{ means don't escape
            (*out) << val->GetString();
        }
    } else if (val->IsInt64()) {
        (*out) << val->GetInt64();
    } else if (val->IsInt()) {
        (*out) << val->GetInt();
    } else if (val->IsDouble()) {
        (*out) << val->GetDouble();
    } else if (val->IsBool()) {
        (*out) << boolalpha << val->GetBool();
    }
    return idx;
}

// Evaluates a LENGTH tag by replacing its contents with the type-dependent 'size' of the
// value.
int EvaluateLength(const string& document, const int idx, const ContextStack* context_stack,
                   const string& tag_name, stringstream* out) {
    const Value* val;
    ResolveJsonContext(tag_name, context_stack, &val);
    if (val == nullptr) return idx;
    if (val->IsArray()) {
        (*out) << val->Size();
    } else if (val->IsString()) {
        (*out) << val->GetStringLength();
    };

    return idx;
}

int EvaluateLiteral(const string& document, const int idx, const ContextStack* context_stack,
                    const string& tag_name, stringstream* out) {
    const Value* val;
    ResolveJsonContext(tag_name, context_stack, &val);
    if (val == nullptr) return idx;
    if (!val->IsArray() && !val->IsObject()) return idx;
    StringBuffer strbuf;
    PrettyWriter<StringBuffer> writer(strbuf);
    val->Accept(writer);
    (*out) << strbuf.GetString();
    return idx;
}

// Evaluates a 'partial' template by reading it fully from disk, then rendering it
// directly into the current output with the current context.
//
// TODO: This could obviously be more efficient (and there are lots of file accesses in a
// long list context).
void EvaluatePartial(const string& tag_name, const string& document_root, const ContextStack* stack,
                     stringstream* out) {
    stringstream ss;
    ss << document_root << tag_name;
    ifstream tmpl(ss.str().c_str());
    if (!tmpl.is_open()) {
        ss << ".mustache";
        tmpl.open(ss.str().c_str());
        if (!tmpl.is_open()) return;
    }
    stringstream file_ss;
    file_ss << tmpl.rdbuf();
    RenderTemplate(file_ss.str(), document_root, stack, out);
}

// Given a tag name, and its operator, evaluate the tag in the given context and write the
// output to 'out'. The heavy-lifting is delegated to specific Evaluate*()
// methods. Returns the new cursor position within 'document', or -1 on error.
int EvaluateTag(const string& document, const string& document_root, int idx,
                const ContextStack* context, const OpCtx& op_ctx, stringstream* out) {
    if (idx == -1) return idx;
    switch (op_ctx.op) {
    case SECTION_START:
    case PREDICATE_SECTION_START:
    case NEGATED_SECTION_START:
    case EQUALITY:
    case INEQUALITY:
        return EvaluateSection(document, document_root, idx, context, op_ctx, out);
    case SUBSTITUTION:
        return EvaluateSubstitution(document, idx, context, op_ctx, out);
    case COMMENT:
        return idx; // Ignored
    case PARTIAL:
        EvaluatePartial(op_ctx.tag_name, document_root, context, out);
        return idx;
    case LENGTH:
        return EvaluateLength(document, idx, context, op_ctx.tag_name, out);
    case LITERAL:
        return EvaluateLiteral(document, idx, context, op_ctx.tag_name, out);
    case NONE:
        return idx; // No tag was found
    case SECTION_END:
        return idx;
    default:
        cout << "Unknown tag: " << op_ctx.op << endl;
        return -1;
    }
}

static bool RenderTemplate(const string& document, const string& document_root,
                           const ContextStack* stack, stringstream* out) {
    int idx = 0;
    while (idx < document.size() && idx != -1) {
        OpCtx op;
        idx = FindNextTag(document, idx, &op, out);
        idx = EvaluateTag(document, document_root, idx, stack, op, out);
    }

    return idx != -1;
}

bool RenderTemplate(const string& document, const string& document_root, const Value& context,
                    stringstream* out) {
    ContextStack stack = {&context, nullptr};
    return RenderTemplate(document, document_root, &stack, out);
}

} // namespace mustache
