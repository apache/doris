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

"""Builds corpora for benchmark_analyzer.hpp: `<name>.rows` files, each row a little-endian u32
length and that many bytes. The corpora are not in the repository; this script makes them.

Sources and licenses of the corpora analyzer_benchmark.md uses:

  zh_wiki, ja_wiki  The plain text of the first articles of the Chinese and Japanese Wikipedia
                    CirrusSearch content dumps of 2025-12-29,
                    https://dumps.wikimedia.org/other/cirrussearch/20251229/, cut at sentence ends
                    into rows of up to 512 bytes. The dump is read as a stream and closed once the
                    corpus is full, so it is never downloaded whole, and the same date gives the
                    same corpus. Wikipedia text is licensed CC BY-SA 4.0,
                    https://creativecommons.org/licenses/by-sa/4.0/.
  http_request      The "request" field of documents-171998.json from the Elastic Rally http_logs
                    track, https://github.com/elastic/rally-tracks/tree/master/http_logs, which is
                    derived from the 1998 World Cup web server logs of the Internet Traffic Archive.
  otel_body         The "Body" field of an OpenTelemetry log sample of the ClickHouse TextBench
                    benchmark data.
  agent_input       The "input" field of an AgentLogs observation file.
  prose_en          Paragraphs of the English ClickHouse documentation (Apache License 2.0),
                    https://github.com/ClickHouse/ClickHouse/tree/master/docs/en.

Usage:
  analyzer_corpus.py wikipedia <zh|ja> <out.rows> [megabytes, default 8]
  analyzer_corpus.py ndjson <file> <field> <out.rows> [megabytes, default 64]
  analyzer_corpus.py paragraphs <glob> <out.rows> [megabytes, default 64]
"""

import glob
import gzip
import json
import re
import struct
import sys
import urllib.request

# A generic client name, as Wikimedia asks every client to send one.
USER_AGENT = "doris-analyzer-benchmark/1.0 (corpus builder for tokenizer benchmarks)"
DUMP_URL = ("https://dumps.wikimedia.org/other/cirrussearch/20251229/"
            "{lang}wiki-20251229-cirrussearch-content.json.gz")
# Sentence ends: CJK full stops and marks, or Latin ones before white space.
SENTENCE_END = re.compile(r"(?<=[。！？])|(?<=[.!?])\s+")


def write_rows(path, rows, limit_bytes):
    """Writes rows until `limit_bytes` of row bytes are written; returns (rows, bytes)."""
    count = 0
    total = 0
    with open(path, "wb") as out:
        for row in rows:
            data = row if isinstance(row, bytes) else row.encode("utf-8", "surrogatepass")
            if not data:
                continue
            out.write(struct.pack("<I", len(data)))
            out.write(data)
            count += 1
            total += len(data)
            if total >= limit_bytes:
                break
    print(f"{path}: {count} rows, {total} bytes")
    return count, total


def sentence_rows(text, max_bytes=512):
    """Packs the sentences of `text` into rows of up to `max_bytes`; a longer sentence is a row."""
    row = ""
    for sentence in SENTENCE_END.split(text):
        if not sentence:
            continue
        if row and len((row + sentence).encode("utf-8")) > max_bytes:
            yield row
            row = ""
        row += sentence
    if row:
        yield row


def wikipedia_rows(lang):
    """Rows of the article texts of a CirrusSearch dump, in the dump's order. Every other line of
    the dump is an index action without text."""
    request = urllib.request.Request(DUMP_URL.format(lang=lang),
                                     headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response, \
            gzip.GzipFile(fileobj=response) as lines:
        for line in lines:
            text = json.loads(line).get("text")
            if isinstance(text, str):
                yield from sentence_rows(text)


def ndjson_rows(path, field):
    with open(path, encoding="utf-8", errors="surrogateescape") as lines:
        for line in lines:
            try:
                value = json.loads(line)[field]
            except (ValueError, KeyError):
                continue
            if isinstance(value, str):
                yield value


def paragraph_rows(pattern):
    for path in sorted(glob.glob(pattern, recursive=True)):
        with open(path, encoding="utf-8", errors="ignore") as text:
            for paragraph in text.read().split("\n\n"):
                paragraph = paragraph.strip()
                if len(paragraph) > 80 and not paragraph.startswith(("```", "|", "<", "---")):
                    yield paragraph


def main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 1
    command = argv[1]
    if command == "wikipedia" and len(argv) in (4, 5):
        megabytes = int(argv[4]) if len(argv) == 5 else 8
        write_rows(argv[3], wikipedia_rows(argv[2]), megabytes << 20)
    elif command == "ndjson" and len(argv) in (5, 6):
        megabytes = int(argv[5]) if len(argv) == 6 else 64
        write_rows(argv[4], ndjson_rows(argv[2], argv[3]), megabytes << 20)
    elif command == "paragraphs" and len(argv) in (4, 5):
        megabytes = int(argv[4]) if len(argv) == 5 else 64
        write_rows(argv[3], paragraph_rows(argv[2]), megabytes << 20)
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
