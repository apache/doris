<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# SNII posting workspace

`MemoryReporter` gives a logical writer one hard posting-workspace budget, currently
32 MiB by default (`snii_postings_workspace_bytes`). Ingestion writers and native
compaction reporters capture this positive byte setting when they are created;
changing it affects new reporters, without resizing an active writer's budget.
This limit applies even when the reporter's overall cap is a
soft ingestion spill threshold. Native compaction can also impose a smaller hard
overall cap. A reservation checks both applicable limits before allocating; an
insufficient budget returns `MEM_LIMIT_EXCEEDED` rather than increasing the limit.

The budget must accommodate the configured compression context as well as posting
buffers. The default accommodates ordinary level-3 encoding. High ZSTD levels can
require substantially more memory: with the bundled ZSTD, a level-19 streaming
context can approach 90 MiB. Such configurations need an explicitly larger budget
(for example, 128 MiB), while a smaller hard overall cap still takes precedence.
The writer reports the configured posting limit on failure and never silently
changes compression level, posting windows, or budget to make a frame fit.

The posting budget includes RUN caches and metadata, merge inputs and heaps,
document/frequency windows, resident position buffers, codec scratch and contexts,
DD staging, window directories/preludes, and retained inline posting payloads.
Replacement reservations include simultaneous old and new capacities. These bytes
also appear once in the parent reporter. The observation callback must not charge
a `MemTrackerLimiter`: Doris's allocation hook already attributes physical memory.

The input compact arena, vocabulary and term ranks, dictionary structures, norms,
and NULL metadata retain their own accounting. They are not covered by the posting
budget. Allocator retention and OS page cache are also separate from this algorithmic
workspace bound; it is not an RSS or whole-BE memory limit.

## Ingestion and temporary runs

`CompactPostingPool` exposes bounded spans of valid tagged-varint payload. Sorted
terms copy those spans directly into `EncodedRunWriter`. The compatibility API's
unsorted input uses bounded external stable sorting by document and arrival order;
ordinary sorted ingestion does not enter that sort path.

Each spill appends an independently sealed run to one writer-owned spool. A
`PostingByteBuffer` directory stores each run's end offset and CRC. Both the directory
and its read cursor share the posting budget and can spill, so neither file names
nor offsets accumulate in a resident array proportional to the run count. A range
is published only after the complete run closes successfully. Appending a later run
does not read or rewrite an earlier prefix.

The private temporary format is independent of persistent SNII files:

- Run header: eight bytes `SNIRUN`, version 2, zero; run seal: `SNIEND`, version 2, zero.
- Term header: varints for term ID, posting shape, document-group count, and token
  count, followed by CRC32C over their canonical varint encoding.
- Fragment: marker 1, varint document-group/token counts and their header CRC,
  followed by payload blocks. Marker 0 ends a term.
- Payload block: varint length (at most 64 KiB), compact payload bytes, and CRC32C.
  A zero block length ends the fragment. Documents can span blocks.
- Spool directory record: little-endian 64-bit end offset and CRC32C over that offset.

Fragment decoding starts independently. Intermediate merges preserve fragment order
and copy encoded blocks; the final consumer combines equal document IDs at adjacent
run boundaries and preserves their position order. Summing fragment document counts
would overestimate the final document frequency and is deliberately not used for it.

`ReducedRuns` merges contiguous ranges in bounded passes. Fan-in is limited by the
remaining workspace and file-descriptor limit, with at least two inputs required.
The historical `snii_spill_max_run_files_per_buffer` setting can further limit fan-in;
zero uses the workspace and descriptor bounds. It no longer causes repeated merging
of an increasingly large ingestion prefix. Intermediate run names live in spillable
manifests, and consumed intermediate files are removed only after the next output
closes successfully. The original spool remains owned by its input buffer.

Production readers require the encoded header and seal, including for empty runs.
The former raw-u32 RUN format is accepted only through explicit `allow_legacy=true`
fixture/diagnostic calls. These temporary files are not used for rolling-upgrade or
persistent-index compatibility.

## Final encoding

`TermPostingBuffer` keeps documents and frequencies separate from replayable
positions. Ordinary windows use the existing resident PRX encoder after admitting
all of its estimated candidates and scratch. Larger position payloads use bounded
RAW/PFOR counting and encoding, plus streaming ZSTD with a reserved context.

Window selection and the persistent FRQ/PRX/DICT layout stay unchanged. DD bytes,
window metadata, super-block directories, and preludes can be staged without
materializing a whole term. All inline DD and PRX payloads share bounded byte and
length streams while waiting for their dictionary block to close. This includes small
payloads and docs-only entries: a large dictionary block cannot retain their
combined bytes outside the posting budget. The streams use at most two temporary files per open block, rather
than one per inline term. Keys and dictionary anchors retain separate dictionary
accounting. Ordinary completed dictionary blocks retain the existing finite
dictionary-output cache; large streamed blocks go directly to its temporary file.

Small admitted windows keep the existing encoded bytes. Streaming ZSTD can produce
different compressed bytes for large windows even with the same codec and level;
byte identity must therefore be checked rather than inferred from format
compatibility. Query comparisons must use one reader against both writer outputs
and include results, scores, window boundaries, codec choices, read/decode counts,
and measured latency.

Successful terminal consumption removes the input spool and directory. I/O,
truncation, checksum, and budget failures propagate as errors; failed intermediate
outputs never replace their inputs. Destructors release reservations and remove
owned temporary files. After a BE crash, the existing `TmpFileDirs::init()` cleanup
removes leftovers on restart. Temporary runs do not publish partially built indexes.
