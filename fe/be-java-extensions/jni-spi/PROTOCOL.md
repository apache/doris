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

# BE ↔ Java plugin protocol

Java source API alone does not describe this boundary. BE resolves method ids by name and
descriptor, reads column data straight out of off-heap memory, and passes schema as strings. None
of that is expressible as a Java signature, so it is written down here. **Everything in this
document has a C++ counterpart and must be changed on both sides in the same commit.**

The Java side of every rule below lives in this module; the C++ side lives in:

| Concern | C++ |
|---|---|
| scanner calls, statistics, scan parameters (v2 path) | `be/src/format_v2/jni/jni_table_reader.cpp` |
| scanner calls, scan parameters (v1 path) | `be/src/format/jni/jni_reader.cpp` |
| off-heap layout, type strings | `be/src/format/jni/jni_data_bridge.h` |
| writer calls | `be/src/format/transformer/vjni_format_transformer.cpp` |

---

## 1. Method set

BE resolves these ids on `JniScanner` / `JniWriter` — never on the concrete plugin class — so the
ABI is fixed by the SPI and a plugin cannot break it. All of them are `final` for the classloader
reason described on `JniScanner`.

### JniScanner

| Method | Descriptor |
|---|---|
| `open` | `()V` |
| `getNextBatchMeta` | `()J` |
| `getTableSchema` | `()Ljava/lang/String;` |
| `getStatistics` | `()Ljava/util/Map;` |
| `getAppendDataTime` | `()J` |
| `getCreateVectorTableTime` | `()J` |
| `setBatchSize` | `(I)V` |
| `releaseColumn` | `(I)V` |
| `releaseTable` | `()V` |
| `close` | `()V` |

### JniWriter

| Method | Descriptor |
|---|---|
| `open` | `()V` |
| `write` | `(Ljava/util/Map;)V` |
| `getStatistics` | `()Ljava/util/Map;` |
| `close` | `()V` |

`JniScannerContractTest` fails if any of these is renamed, re-signed, moved off the base class, or
made overridable.

---

## 2. Batch lifecycle and the end-of-stream rule

```
open()
repeat:
    address = getNextBatchMeta()      // 0 means end of stream
    if address == 0: break
    ... BE reads the batch through the meta array ...
    releaseColumn(i) per column BE is done with, then releaseTable()
close()
```

`getNextBatchMeta()` returning **0 is end of stream, not an error and not an address**. By the time
it returns 0 the vector table has already been released, so BE must not dereference anything from
the previous batch afterwards. The same applies when the scan throws: the table is released before
the exception propagates.

---

## 3. Reserved parameter keys

The scan parameter map is otherwise private between a connector's FE side and its plugin, but these
keys are set by BE and must not be reused for anything else.

| Key | Direction | Meaning |
|---|---|---|
| `required_fields` | BE → plugin | comma-separated column names, in output order |
| `columns_types` | BE → plugin | `#`-separated type strings, positionally paired with `required_fields` |
| `required_fields_base64` | BE → plugin | same names, base64-encoded; only published for connectors that opt in |
| `columns_types_base64` | BE → plugin | same types with base64-encoded struct field names, preserving their exact spelling — the plain grammar lowercases them |
| `replace_string` | BE → plugin | comma-separated per-column replacement directives; absent when every column is `not_replace` |
| `time_zone` | BE → plugin | session time zone; query-scoped, so it overwrites any catalog-level copy |
| `meta_address` | BE → writer | address of the meta array of the block being written |
| `num_rows` | BE → writer | row count of that block |

Separators are part of the protocol: `,` between names, `#` between types. A column name containing
either is only transportable through the base64 pair.

---

## 4. Type strings

One string per column, parsed by `ColumnType.parseType`. Case-insensitive.

```
type      := scalar | sized | decimal | complex
scalar    := boolean | tinyint | smallint | int | bigint | largeint | float | double
           | ipv4 | ipv6 | string | binary | bytes | varbinary
           | date | datev1 | datev2 | datetimev1
sized     := char(N) | varchar(N) | varbinary(N)
           | timestamp[(P)] | datetime[(P)] | datetimev2[(P)] | timestamptz[(P)]     -- P defaults to 6
decimal   := decimal(P,S) | decimalv2(P,S) | decimal32(P,S) | decimal64(P,S) | decimal128(P,S)
complex   := array<type>
           | map<type,type>
           | struct<name:type[,name:type]...>
```

Notes that have bitten people:

- Bare `decimal(P,S)` picks the physical type from the precision: ≤9 → decimal32, ≤18 → decimal64,
  otherwise decimal128. A plugin must not assume the width it asked for.
- `date`/`datev2` both mean DATEV2; `datev1` is the deprecated 8-byte form. Same for
  `datetimev1` vs `datetime`/`datetimev2`.
- Struct field names are **lowercased** by the plain grammar. Only the base64 variant round-trips
  the original spelling, and each encoded name is prefixed with `$` as a version marker.
- Nesting is parsed by bracket counting, so `map<string,array<int>>` is fine, but a struct field
  name containing `<`, `>`, `,` or `:` is not representable in the plain grammar.
- An unrecognised string is not an error: it becomes `UNSUPPORTED`, whose meta contribution is a
  single 0 (see below). Silently getting nulls for a column usually means a typo here.

---

## 5. Meta array

`getNextBatchMeta()` returns the address of an array of 64-bit little-endian words:

```
[0]  = number of rows in this batch
[1..] = per column, in required_fields order, the words its type contributes
```

What a column contributes, and how many words (`ColumnType.metaSize()`):

| Type class | Words | Contents |
|---|---|---|
| unsupported | 2 | `0` (the column is absent; the second word is the const flag slot) |
| fixed width | 3 | nullMap address, data address |
| string / char / varchar | 4 | nullMap address, offsets address, data address |
| array, map | 3 + children | nullMap address, offsets address, then each child recursively |
| struct | 2 + children | nullMap address, then each child recursively |

The word count in the table includes the const-flag slot that `metaSize()` accounts for, which is
why it is one more than the number of addresses listed. A null address means "no null map" (the
column has no nulls) — it is not an error.

### The two directions do not use the same layout

The table above is the **writer** direction: C++ builds the meta array in
`JniDataBridge::_fill_column_meta`, which writes the const flag, and Java reads it in
`VectorColumn`'s readable constructor, which reads the const flag.

The **scanner** direction omits that word. `VectorColumn.updateMeta` writes only the addresses, and
`JniDataBridge::fill_column` reads only the addresses. Each direction is self-consistent, so both
work, but they are not the same layout and `metaSize()` describes only the first of them.

The practical consequence: **a meta address obtained from a writable `VectorTable` cannot be read
back by `VectorTable.createReadableTable`**. Doing so shifts every column by one word, so the const
flag is read out of an address (making every column look constant) and the data pointer is read out
of whatever follows the array — a segfault, or silently wrong data. A test that wants to feed a
writer must build the meta array the way `_fill_column_meta` does; `JavaWriterPluginTest` in the
java-writer plugin does exactly that and is the worked example.

**This layout is duplicated in C++ in `jni_data_bridge.h`.** There is no runtime check that the two
agree; a mismatch reads whatever memory the wrong offset lands on.

---

## 6. Statistics keys

`getStatistics()` returns `Map<String,String>` where the key is `metricType:metricName` and the
value is a decimal integer. BE adds `metricName` under the scanner's profile node, labelled with the
connector name.

| metricType | Profile unit | Update rule |
|---|---|---|
| `timer` | time | accumulate |
| `counter` | count | accumulate |
| `bytes` | bytes | accumulate |
| `timer_gauge` | time | replace |
| `gauge` | count | replace |
| `bytes_gauge` | bytes | replace |
| `timer_peak` | time | keep the maximum |
| `peak` | count | keep the maximum |
| `bytes_peak` | bytes | keep the maximum |

A key with no `:`, or with an unknown type, is logged as a warning and dropped. Metrics never fail a
query, so a metric that never appears in a profile is a typo here rather than a runtime error.

---

## 7. Plugin discovery and version

- A plugin is a directory under `lib/java/plugins/`; the directory name is the name BE addresses it
  by. All jars in the directory form its classpath.
- The entry point is found by `ServiceLoader`, from
  `META-INF/services/org.apache.doris.jni.spi.DorisPlugin` inside the plugin's own jars.
- Within a plugin, each factory is addressed by its `getName()`. BE sends the pair
  (plugin directory, factory name).
- The plugin declares the API version it was built against in the MANIFEST attribute
  `Doris-Jni-Plugin-Api-Version` of the jar that defines its `DorisPlugin`. BE compares the major
  with its own, which it reads from `META-INF/doris/jni-plugin-api-version.properties` in this jar.
  Both numbers come from `jni.plugin.api.version` in `fe/be-java-extensions/pom.xml`. See
  `SpiVersion` for why the plugin side has to be a MANIFEST attribute and not a method.
