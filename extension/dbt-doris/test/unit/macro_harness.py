#!/usr/bin/env python
# encoding: utf-8

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

"""Render dbt-doris Jinja macros in isolation, with no Doris cluster.

The functional suite under ``test/functional`` needs a reachable cluster, so it
cannot guard a pull request. These helpers make the macros themselves testable:
they compile the shipped .sql files with dbt's own Jinja environment and run a
single macro against a hand-built context.

What that catches: Jinja syntax errors, macros that emit more than one SQL
statement, unescaped values interpolated into SQL, and dispatch/naming mistakes.
What it does not catch: anything about how Doris executes the SQL. Keep
behavioural coverage in the functional suite.
"""

import os
import re
from typing import Any, Dict, List, Optional

from dbt_common.clients.jinja import extract_toplevel_blocks, get_environment
from dbt_common.exceptions.macros import MacroReturn
from dbt_common.utils.jinja import MACRO_PREFIX

from dbt.include import doris as doris_include

MACRO_ROOT = os.path.join(doris_include.PACKAGE_PATH, "macros")

#: Blocks dbt itself allows at the top level of a macro file.
ALLOWED_BLOCKS = {"macro", "materialization", "test", "docs", "snapshot"}


def macro_files() -> List[str]:
    """Every .sql file shipped in the adapter's macro directory, repo-relative."""
    found = []
    for dirpath, _, filenames in os.walk(MACRO_ROOT):
        for filename in filenames:
            if filename.endswith(".sql"):
                found.append(
                    os.path.relpath(os.path.join(dirpath, filename), MACRO_ROOT)
                )
    return sorted(found)


def read_macro_file(rel_path: str) -> str:
    with open(os.path.join(MACRO_ROOT, rel_path)) as fh:
        return fh.read()


def top_level_blocks(rel_path: str):
    """Parse a macro file into its top-level macro/materialization blocks."""
    return extract_toplevel_blocks(
        read_macro_file(rel_path),
        allowed_blocks=ALLOWED_BLOCKS,
        collect_raw_data=False,
    )


class Statement:
    """One ``{% call statement(...) %}`` block captured during a render."""

    def __init__(self, name: Optional[str], sql: str):
        self.name = name
        self.sql = sql.strip()

    def __repr__(self) -> str:
        return f"Statement(name={self.name!r}, sql={self.sql!r})"


class FakeConfig:
    """Stand-in for dbt's model ``config`` object.

    ``validator`` is accepted and ignored: the macros pass
    ``validation.any[...]`` to it, and validating the fake values here would only
    test dbt, not the adapter.
    """

    def __init__(self, values: Optional[Dict[str, Any]] = None):
        self.values = dict(values or {})

    def get(self, name, default=None, validator=None):
        return self.values.get(name, default)

    def require(self, name, validator=None):
        return self.values[name]

    def persist_relation_docs(self):
        return self.values.get("persist_docs", {}).get("relation", False)

    def persist_column_docs(self):
        return self.values.get("persist_docs", {}).get("columns", False)


class _AnyValidator:
    """``validation.any[list, basestring]`` -- subscript, then ignore."""

    def __getitem__(self, item):
        def noop(value):
            return None

        return noop


class FakeValidation:
    any = _AnyValidator()


class FakeRelation:
    """Minimal Relation: enough for the string interpolation the macros do."""

    def __init__(self, schema="dbt_test", identifier="my_model", relation_type="table"):
        self.schema = schema
        self.identifier = identifier
        self.table = identifier
        self.type = relation_type

    @property
    def is_view(self):
        return self.type == "view"

    def include(self, database=True, schema=True, identifier=True):
        return self

    def incorporate(self, **kwargs):
        path = kwargs.get("path") or {}
        return FakeRelation(
            schema=path.get("schema", self.schema),
            identifier=path.get("identifier", self.identifier),
            relation_type=kwargs.get("type", self.type),
        )

    def render(self):
        return str(self)

    def __str__(self):
        return f"`{self.schema}`.`{self.identifier}`"


class FakeColumn:
    """Minimal dbt Column used by SQL-producing strategy macro tests."""

    def __init__(self, name):
        self.name = name

    @property
    def quoted(self):
        return f"`{self.name}`"


class FakeAdapter:
    """Small adapter surface shared by isolated SQL macro tests."""

    @staticmethod
    def quote(identifier):
        return f"`{identifier}`"


class FakeRow:
    """Stand-in for an agate Row, as returned by ``run_query``.

    Iteration yields *values*, not keys -- the partition macros rely on that.
    """

    def __init__(self, mapping: Dict[str, Any]):
        self._mapping = dict(mapping)

    def __iter__(self):
        return iter(self._mapping.values())

    def __len__(self):
        return len(self._mapping)

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self._mapping.values())[key]
        return self._mapping[key]

    def items(self):
        return self._mapping.items()

    def keys(self):
        return self._mapping.keys()

    def values(self):
        return self._mapping.values()


class CapturedCompilerError(Exception):
    """Raised in place of dbt's compiler error, so tests can assert on it."""


class FakeExceptions:
    @staticmethod
    def raise_compiler_error(msg, node=None):
        raise CapturedCompilerError(msg)

    @staticmethod
    def raise_not_implemented(msg):
        raise CapturedCompilerError(msg)

    @staticmethod
    def raise_fail_fast_error(msg, node=None):
        raise CapturedCompilerError(msg)

    @staticmethod
    def warn(msg):
        return None


class MacroRunner:
    """Compile a macro file and call one macro out of it.

    Macros in the same file call each other by bare name, while dbt's Jinja
    parser renames every *definition* to ``dbt_macro__<name>``. Bare names are
    resolved from the render context in real dbt, so the runner re-binds each
    compiled macro into the context under its bare name before rendering. That
    is what lets, say, ``doris__create_table_as`` reach ``doris__properties``.
    """

    def __init__(self, *rel_paths: str, context: Optional[Dict[str, Any]] = None):
        self.rel_paths = rel_paths
        self.source = "\n".join(read_macro_file(p) for p in rel_paths)
        self.statements: List[Statement] = []
        self.context: Dict[str, Any] = self._base_context()
        self.context.update(context or {})

    def _base_context(self) -> Dict[str, Any]:
        def statement(name=None, fetch_result=False, auto_begin=True, caller=None):
            sql = caller() if caller is not None else ""
            self.statements.append(Statement(name, sql))
            return ""

        def do_return(value):
            raise MacroReturn(value)

        return {
            "statement": statement,
            "return": do_return,
            "config": FakeConfig(),
            "validation": FakeValidation(),
            "exceptions": FakeExceptions(),
            "model": {},
            "basestring": str,
            "modules": {"re": re},
            "log": lambda msg, info=False: "",
        }

    @staticmethod
    def _catch_return(macro):
        """Mirror dbt's MacroGenerator: ``return()`` ends one macro, not the stack.

        ``return()`` raises MacroReturn, and dbt catches it around each macro
        call. Without this wrapper a ``return()`` in a nested macro -- say
        ``get_partition_items`` -- would unwind the caller's loop as well.
        """

        def wrapper(*args, **kwargs):
            try:
                return macro(*args, **kwargs)
            except MacroReturn as e:
                return e.value

        return wrapper

    def render(self, macro_name: str, *args, **kwargs):
        """Call ``macro_name``; return its ``return()`` value, else its output."""
        env = get_environment(None, capture_macros=False)
        template = env.from_string(self.source)
        # A module render binds the macros; MacroFuzzTemplate.new_context takes
        # the context dict by reference, so later mutations are visible to the
        # macro bodies.
        module = template.make_module(self.context)
        macros = {}
        for attr in dir(module):
            if attr.startswith(MACRO_PREFIX):
                bare = attr[len(MACRO_PREFIX) :]
                macros[bare] = getattr(module, attr)
                self.context[bare] = self._catch_return(macros[bare])

        macro = macros.get(macro_name)
        if macro is None:
            raise AssertionError(
                f"macro {macro_name!r} is not defined in {', '.join(self.rel_paths)}"
            )
        self.statements = []
        try:
            return str(macro(*args, **kwargs))
        except MacroReturn as e:
            return e.value

    def sql(self, macro_name: str, *args, **kwargs) -> str:
        """Render and normalise whitespace, for readable assertions."""
        return " ".join(self.render(macro_name, *args, **kwargs).split())
