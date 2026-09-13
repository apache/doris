#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Pairing guard for `extern template` families.

The BE uses extern-template families (ColumnVector, ColumnDecimal, the operator
and serde stacks, ...) to instantiate hot template specializations once instead
of in every translation unit. Of all the ways such a pairing can rot, exactly
one is silent: a .cpp carries the explicit instantiation definition but the
header lost (or never gained) the matching `extern template` declaration. The
code still compiles and links -- every TU just quietly goes back to implicitly
instantiating that specialization, and the build slows down with no diagnostic
anywhere. This script turns both pairing directions into an error:

  forward   every `extern template` declaration in a be/src header must have a
            matching explicit instantiation definition in some be/src .cpp
            (missing ones eventually break the link, but only when a TU odr-uses
            a non-inline member -- this reports them at once, with the file the
            definition should go in).
  reverse   within a COVERED family -- a template name that already has at least
            one extern declaration -- every explicit instantiation definition
            must have a matching extern declaration (the silent case). Families
            with no externs at all are intentionally out of scope: instantiation
            without extern is a legitimate TU-local idiom there.

Matching is textual, on a normalized signature: whitespace folded, comments
stripped, namespace qualifiers dropped, known type aliases canonicalized, and
multi-line statements joined. Zero build dependency; runs in about half a
second.

Usage:
    build-support/check-extern-template-pairing.py            # enforce
    build-support/check-extern-template-pairing.py --list     # dump the pairing tables
"""

import argparse
import collections
import os
import re
import sys

SRC_ROOT = "be/src"

# Spellings that legitimately differ between a declaration site and its
# definition site are rewritten to one canonical form ON BOTH SIDES before
# matching. Because the rewrite is uniform, a pair that already spells its
# arguments consistently can never be broken by an entry here; entries are only
# ever needed when the header and the .cpp spell the same type differently.
# Keep this table small -- the better fix for a new mismatch is to spell both
# sides identically.
TYPE_ALIASES = {
    # core/types.h vectorized aliases vs the underlying fixed-width types
    # (column_string.h declares ColumnStr<UInt32>, column_string.cpp
    # instantiates ColumnStr<uint32_t>).
    "UInt32": "uint32_t",
    "UInt64": "uint64_t",
    # wide:: aliases vs the underlying wide::integer specializations
    # (wide_integer_to_string.h declares to_string(const Int128&), the .cpp
    # instantiates to_string(const integer<128, signed>&)).
    "Int128": "integer<128,signed>",
    "UInt128": "integer<128,unsigned>",
    "Int256": "integer<256,signed>",
    "UInt256": "integer<256,unsigned>",
}

# Escape hatch, deliberately empty. An entry is
#     ("be/src/path/to/file.ext", "<normalized signature>")
# as printed by a failure (or by --list), and silences that one declaration or
# definition. Adding one must be a reviewed decision with a comment saying why
# the pairing is intentionally broken.
ALLOW = set()

EXTERN_START = re.compile(r"^\s*extern\s+template\b")
# An explicit instantiation begins `template` followed by anything but `<`
# (`template <...>` opens a template definition, not an instantiation).
DEF_START = re.compile(r"^\s*template\s+[A-Za-z_:]")
LINE_COMMENT = re.compile(r"//.*$")
BLOCK_COMMENT = re.compile(r"/\*.*?\*/")


def statements(path, start):
    """Yields (lineno, statement) for namespace-scope statements matching
    `start`, joining continuation lines up to the terminating semicolon and
    skipping preprocessor directives and macro bodies (an instantiation-shaped
    line inside a #define is not an instantiation)."""
    with open(path, encoding="utf-8", errors="ignore") as handle:
        lines = handle.read().splitlines()
    in_macro = False
    i = 0
    while i < len(lines):
        raw = lines[i]
        if in_macro or raw.lstrip().startswith("#"):
            in_macro = raw.rstrip().endswith("\\")
            i += 1
            continue
        if not start.match(raw):
            i += 1
            continue
        stmt, j = raw, i
        while ";" not in stmt and j + 1 < len(lines) and j - i < 20:
            j += 1
            stmt += " " + lines[j]
        if ";" in stmt and "\\" not in stmt:
            yield i + 1, stmt.split(";")[0].strip()
        i = j + 1


def normalize(sig):
    """Collapses a declaration/definition to the comparable skeleton."""
    s = BLOCK_COMMENT.sub(" ", sig)
    s = LINE_COMMENT.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^extern\s+", "", s)
    s = re.sub(r"^template\s+", "", s)
    s = re.sub(r"^(class|struct)\s+", "", s)
    s = re.sub(r"\b[A-Za-z_]\w*::", "", s)
    for alias, canonical in TYPE_ALIASES.items():
        s = re.sub(rf"\b{alias}\b", canonical, s)
    return s.replace(" ", "")


def family(norm):
    """The template name a normalized signature instantiates: the identifier
    before the argument list for a type, the function name for a function."""
    paren = norm.find("(")
    if paren != -1:
        m = re.search(r"([A-Za-z_]\w*)(?:<[^()]*>)?\($", norm[: paren + 1])
        if m:
            return m.group(1)
    m = re.match(r"([A-Za-z_]\w*)<", norm)
    return m.group(1) if m else norm


def walk(suffixes):
    for directory, _, names in os.walk(SRC_ROOT):
        for name in names:
            if name.endswith(suffixes):
                yield os.path.join(directory, name)


def collect():
    externs, defs = [], []
    for path in walk((".h", ".hpp")):
        with open(path, encoding="utf-8", errors="ignore") as handle:
            if "extern template" not in handle.read():
                continue
        for lineno, stmt in statements(path, EXTERN_START):
            externs.append((path, lineno, stmt, normalize(stmt)))
    for path in walk((".cpp", ".cc")):
        for lineno, stmt in statements(path, DEF_START):
            if re.match(r"^\s*extern\b", stmt):
                continue
            defs.append((path, lineno, stmt, normalize(stmt)))
    return externs, defs


def enforce(externs, defs):
    def_index = collections.defaultdict(list)
    for path, lineno, stmt, norm in defs:
        def_index[norm].append((path, lineno))
    extern_index = collections.defaultdict(list)
    covered = collections.defaultdict(set)  # family -> headers declaring it
    for path, lineno, stmt, norm in externs:
        extern_index[norm].append((path, lineno))
        covered[family(norm)].add(path)

    failures = 0
    for path, lineno, stmt, norm in externs:
        if norm in def_index or (path, norm) in ALLOW:
            continue
        failures += 1
        print(
            "error: extern template declaration pairs with no explicit "
            "instantiation definition",
            file=sys.stderr,
        )
        print(f"  decl:   {path}:{lineno}: {stmt};", file=sys.stderr)
        print(f"  norm:   {norm}", file=sys.stderr)
        print(
            "  reason: 'extern template' promises the specialization is "
            "instantiated in some .cpp; without one, any TU that odr-uses a "
            "non-inline member fails to link -- and if every member is inline, "
            "the declaration is dead weight",
            file=sys.stderr,
        )
        print(
            "  fix:    add the matching 'template class/struct/... ;' to the "
            ".cpp that owns this family, or delete the declaration; an "
            "intentionally unpaired declaration goes in ALLOW with a comment",
            file=sys.stderr,
        )
    for path, lineno, stmt, norm in defs:
        fam = family(norm)
        if fam not in covered or norm in extern_index or (path, norm) in ALLOW:
            continue
        failures += 1
        headers = ", ".join(sorted(covered[fam]))
        print(
            "error: explicit instantiation lacks the matching 'extern "
            "template' declaration",
            file=sys.stderr,
        )
        print(f"  def:    {path}:{lineno}: {stmt};", file=sys.stderr)
        print(f"  norm:   {norm}", file=sys.stderr)
        print(f"  family: {fam} is extern-covered in {headers}", file=sys.stderr)
        print(
            "  reason: every extern-declared specialization of this family is "
            "instantiated once, but this one re-instantiates in every "
            "including TU -- the one pairing mistake that is silent: it "
            "compiles, links, and only slows the build down",
            file=sys.stderr,
        )
        print(
            "  fix:    add 'extern template ...;' next to the family's other "
            "externs in the header above, spelling the template arguments the "
            "same way as the definition, or list the definition in ALLOW with "
            "a comment",
            file=sys.stderr,
        )
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--list",
        action="store_true",
        help="dump every declaration and definition with its normalized form",
    )
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(root)
    externs, defs = collect()

    if args.list:
        for path, lineno, stmt, norm in externs:
            print(f"decl {path}:{lineno}\n     {norm}")
        for path, lineno, stmt, norm in defs:
            print(f"def  {path}:{lineno}\n     {norm}")
        return 0

    failures = enforce(externs, defs)
    if failures:
        print(f"\n{failures} extern/instantiation pairing violation(s)", file=sys.stderr)
        return 1
    families = {family(n) for _, _, _, n in externs}
    print(
        f"extern/instantiation pairing: {len(externs)} declaration(s), "
        f"{len(families)} covered family(s), all paired"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
