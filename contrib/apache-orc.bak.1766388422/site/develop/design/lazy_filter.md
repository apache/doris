---
layout: page
title: Lazy Filter
---

* [Background](#Background)
* [Design](#Design)
  * [SArg to Filter](#SArgtoFilter)
  * [Read](#Read)
* [Configuration](#Configuration)
* [Tests](#Tests)
* [Appendix](#Appendix)
  * [Benchmarks](#Benchmarks)
    * [Row vs Vector](#RowvsVector)
    * [Normalization vs Compact](#NormalizationvsCompact)
    * [Summary](#Summary)

## Background <a id="Background"></a>

This feature request started as a result of a needle in the haystack search that is performed with the following
characteristics:

* The search fields are not part of partition, bucket or sort specification.
* The table is a very large table.
* The result is very few rows compared to the scan size.
* The search columns are a significant subset of selection columns in the query.

Initial analysis showed that we could have a significant benefit by lazily reading the non-search columns only when we
have a match. We explore the design and some benchmarks in subsequent sections.

## Design <a id="Design"></a>

This builds further on [ORC-577][ORC-577] which currently only restricts deserialization for some selected data types
but does not improve on IO.

On a high level the design includes the following components:

```text
┌──────────────┐          ┌────────────────────────┐
│              │          │          Read          │
│              │          │                        │
│              │          │     ┌────────────┐     │
│SArg to Filter│─────────▶│     │Read Filter │     │
│              │          │     │  Columns   │     │
│              │          │     └────────────┘     │
│              │          │            │           │
└──────────────┘          │            ▼           │
                          │     ┌────────────┐     │
                          │     │Apply Filter│     │
                          │     └────────────┘     │
                          │            │           │
                          │            ▼           │
                          │     ┌────────────┐     │
                          │     │Read Select │     │
                          │     │  Columns   │     │
                          │     └────────────┘     │
                          │                        │
                          │                        │
                          └────────────────────────┘
```

* **SArg to Filter**: Converts Search Arguments passed down into filters for efficient application during scans.
* **Read**: Performs the lazy read using the filters.
  * **Read Filter Columns**: Read the filter columns from the file.
  * **Apply Filter**: Apply the filter on the read filter columns.
  * **Read Select Columns**: If filter selects at least a row then read the remaining columns.

### SArg to Filter <a id="SArgtoFilter"></a>

SArg to Filter converts the passed SArg into a filter. This enables automatic compatibility with both Spark and Hive as
they already push down Search Arguments down to ORC.

The SArg is automatically converted into a [Vector Filter][vfilter]. Which is applied during the read process. Two
filter types were evaluated:

* [Row Filter][rfilter] that evaluates each row across all the predicates once.
* [Vector Filter][vfilter] that evaluates each filter across the entire vector and adjusts the subsequent evaluation.

While a row based filter is easier to code, it is much [slower][rowvvector] to process. We also see a significant
[performance gain][rowvvector] in the absence of normalization.

The builder for search argument should allow skipping normalization during the [build][build]. This has been added with
[HIVE-24458][HIVE-24458].

### Read <a id="Read"></a>

The read process has the following changes:

```text
                         │
                         │
                         │
┌────────────────────────▼────────────────────────┐
│               ┏━━━━━━━━━━━━━━━━┓                │
│               ┃Plan ++Search++ ┃                │
│               ┃    Columns     ┃                │
│               ┗━━━━━━━━━━━━━━━━┛                │
│                 Read   │Stripe                  │
└────────────────────────┼────────────────────────┘
                         │
                         ▼


                         │
                         │
┌────────────────────────▼────────────────────────┐
│               ┏━━━━━━━━━━━━━━━━┓                │
│               ┃Read ++Search++ ┃                │
│               ┃    Columns     ┃◀─────────┐     │
│               ┗━━━━━━━━━━━━━━━━┛          │     │
│                        │              Size = 0  │
│                        ▼                  │     │
│               ┏━━━━━━━━━━━━━━━━┓          │     │
│               ┃  Apply Filter  ┃──────────┘     │
│               ┗━━━━━━━━━━━━━━━━┛                │
│                    Size > 0                     │
│                        │                        │
│                        ▼                        │
│               ┏━━━━━━━━━━━━━━━━┓                │
│               ┃  Plan Select   ┃                │
│               ┃    Columns     ┃                │
│               ┗━━━━━━━━━━━━━━━━┛                │
│                        │                        │
│                        ▼                        │
│               ┏━━━━━━━━━━━━━━━━┓                │
│               ┃  Read Select   ┃                │
│               ┃    Columns     ┃                │
│               ┗━━━━━━━━━━━━━━━━┛                │
│                   Next │Batch                   │
└────────────────────────┼────────────────────────┘
                         │
                         ▼
```

The read process changes:

* **Read Stripe** used to plan the read of all (search + select) columns. This is enhanced to plan and fetch only the
  search columns. The rest of the stripe planning process optimizations remain unchanged e.g. partial read planning of
  the stripe based on RowGroup statistics.
* **Next Batch** identifies the processing that takes place when `RecordReader.nextBatch` is invoked.
  * **Read Search Columns** takes place instead of reading all the selected columns. This is in sync with the planning
    that has taken place during **Read Stripe** where only the search columns have been planned.
  * **Apply Filter** on the batch that at this point only includes search columns. Evaluate the result of the filter:
    * **Size = 0** indicates all records have been filtered out. Given this we proceed to the next batch of search
      columns.
    * **Size > 0** indicates that at least one record accepted by the filter. This record needs to be substantiated with
      other columns.
  * **Plan Select Columns** is invoked to perform read of the select columns. The planning happens as follows:
    * Determine the current position of the read within the stripe and plan the read for the select columns from this
      point forward to the end of the stripe.
    * The Read planning of select columns respects the row groups filtered out as a result of the stripe planning.
    * Fetch the select columns using the above plan.
  * **Read Select Columns** into the vectorized row batch
  * Return this batch.

The current implementation performs a single read for the select columns in a stripe.

```text
┌──────────────────────────────────────────────────┐
│ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ │
│ │RG0 │ │RG1 │ │RG2■│ │RG3 │ │RG4 │ │RG5■│ │RG6 │ │
│ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ │
│                      Stripe                      │
└──────────────────────────────────────────────────┘
```

The above diagram depicts a stripe with 7 Row Groups out of which **RG2** and **RG5** are selected by the filter. The
current implementation does the following:

* Start the read planning process from the first match RG2
* Read to the end of the stripe that includes RG6
* Based on the above fetch skips RG0 and RG1 subject to compression block boundaries

The above logic could be enhanced to perform say **2 or n** reads before reading to the end of stripe. The current
implementation allows 0 reads before reading to the end of the stripe. The value of **n** could be configurable but
should avoid too many short reads.

The read behavior changes as follows with multiple reads being allowed within a stripe for select columns:

```text
┌──────────────────────────────────────────────────┐
│ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ │
│ │    │ │    │ │■■■■│ │■■■■│ │■■■■│ │■■■■│ │■■■■│ │
│ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ │
│              Current implementation              │
└──────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────┐
│ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ │
│ │    │ │    │ │■■■■│ │    │ │    │ │■■■■│ │■■■■│ │
│ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ │
│               Allow 1 partial read               │
└──────────────────────────────────────────────────┘
```

The figure shows that we could read significantly fewer bytes by performing an additional read before reading to the end
of stripe. This shall be included as a subsequent enhancement to this patch.

## Configuration <a id="Configuration"></a>

The following configuration options are exposed that control the filter behavior:

|Property               |Type   |Default|
|:---                   |:---   |:---   |
|orc.sarg.to.filter     |boolean|false  |
|orc.filter.use.selected|boolean|false  |

* `orc.sarg.to.filter` can be used to turn off the SArg to filter conversion. This might be particularly relevant in
  cases where the filter is expensive and does not eliminate a lot of records. This will not be relevant once we have
  the option to turn off the filters on the caller as they have been completely implemented by the ORC layer.
* `orc.filter.use.selected` is an important setting that if incorrectly enabled results in wrong output. A boolean flag
  to determine if the selected vector is supported by the reading application. If false, the output of the ORC reader
  must have the filter reapplied to avoid using unset values in the unselected rows. If unsure please leave this as
  false.

## Tests <a id="Tests"></a>

We evaluated this patch against a search job with the following stats:

* Table
  * Size: ~**420 TB**
  * Data fields: ~**120**
  * Partition fields: **3**
* Scan
  * Search fields: 3 data fields with large (~ 1000 value) IN clauses compounded by **OR**.
  * Select fields: 16 data fields (includes the 3 search fields), 1 partition field
  * Search:
    * Size: ~**180 TB**
    * Records: **3.99 T**
  * Selected:
    * Size: ~**100 MB**
    * Records: **1 M**

We have observed the following reductions:

|Test          |IO Reduction %|CPU Reduction %|
|:---          |          ---:|           ---:|
|SELECT 16 cols|            45|             47|
|SELECT *      |            70|             87|

* The savings are more significant as you increase the number of select columns with respect to the search columns
* When the filter selects most data, no significant penalty observed as a result of 2 IO compared with a single IO
  * We do have a penalty as a result of the double filter application both in ORC and in the calling engine.

## Appendix <a id="Appendix"></a>

### Benchmarks <a id="Benchmarks"></a>

#### Row vs Vector <a id="RowvsVector"></a>

We start with a decision of using a Row filter vs a Vector filter. The Row filter has the advantage of simpler code when
compared with the Vector filter.

```bash
java -jar java/bench/core/target/orc-benchmarks-core-*-uber.jar filter simple
```

|Benchmark   |(fInSize)|(fType)|Mode| Cnt| Score|Error  |Units|
|:---        |     ---:|:---   |:---|---:|  ---:|:---   |:--- |
|SimpleFilter|        4|row    |avgt|  20|38.207|± 0.178|us/op|
|SimpleFilter|        4|vector |avgt|  20|18.663|± 0.117|us/op|
|SimpleFilter|        8|row    |avgt|  20|50.694|± 0.313|us/op|
|SimpleFilter|        8|vector |avgt|  20|35.532|± 0.190|us/op|
|SimpleFilter|       16|row    |avgt|  20|52.443|± 0.268|us/op|
|SimpleFilter|       16|vector |avgt|  20|33.966|± 0.204|us/op|
|SimpleFilter|       32|row    |avgt|  20|68.504|± 0.318|us/op|
|SimpleFilter|       32|vector |avgt|  20|51.707|± 0.302|us/op|
|SimpleFilter|      256|row    |avgt|  20|88.348|± 0.793|us/op|
|SimpleFilter|      256|vector |avgt|  20|72.602|± 0.282|us/op|

Explanation:

* **fInSize** calls out the number of values in the IN clause.
* **fType** calls out the whether the filter is a row based filter, or a vector based filter.

Observations:

* The vector based filter is significantly faster than the row based filter.
  * At best, vector was faster by **51.15%**
  * At worst, vector was faster by **17.82%**
* The performance of the filters is deteriorates with the increase of the IN values, however even in this case the
  vector filter is much better than the row filter. The current `IN` filter employs a binary search on an array instead
  of a hash lookup.

#### Normalization vs Compact <a id="NormalizationvsCompact"></a>

In this test we use a complex filter with both AND, and OR to understand the impact of Conjunctive Normal Form on the
filter performance. The Search Argument builder by default performs a CNF. The advantage of the CNF would again be a
simpler code base.

```bash
java -jar java/bench/core/target/orc-benchmarks-core-*-uber.jar filter complex
```

|Benchmark    |(fSize)|(fType)|(normalize)|Mode| Cnt|   Score|Error   |Units|
|:---         |   ---:|:---   |:---       |:---|---:|    ---:|:---    |:--- |
|ComplexFilter|      2|row    |true       |avgt|  20|  91.922|± 0.301 |us/op|
|ComplexFilter|      2|row    |false      |avgt|  20|  90.741|± 0.556 |us/op|
|ComplexFilter|      2|vector |true       |avgt|  20|  61.137|± 0.398 |us/op|
|ComplexFilter|      2|vector |false      |avgt|  20|  54.829|± 0.431 |us/op|
|ComplexFilter|      4|row    |true       |avgt|  20| 284.956|± 1.237 |us/op|
|ComplexFilter|      4|row    |false      |avgt|  20| 130.526|± 0.767 |us/op|
|ComplexFilter|      4|vector |true       |avgt|  20| 242.387|± 1.053 |us/op|
|ComplexFilter|      4|vector |false      |avgt|  20|  98.530|± 0.423 |us/op|
|ComplexFilter|      8|row    |true       |avgt|  20|8007.101|± 54.912|us/op|
|ComplexFilter|      8|row    |false      |avgt|  20| 234.943|± 4.713 |us/op|
|ComplexFilter|      8|vector |true       |avgt|  20|7013.758|± 33.701|us/op|
|ComplexFilter|      8|vector |false      |avgt|  20| 190.442|± 0.881 |us/op|

Explanation:

* **fSize** identifies the size of the children in the OR clause that will be normalized.
* **normalize** identifies whether normalize was carried out on the Search Argument.

Observations:

* Vector filter is better than the row filter as demonstrated by the [Row vs Vector Test][rowvvector].
* Normalizing the search argument results in a significant performance penalty given the explosion of the operator tree
  * In case where an AND includes 8 ORs, the compact version is faster by **97.29%**

#### Summary <a id="Summary"></a>

Based on the benchmarks we have the following conclusions:

* Vector based filter is significantly better than a row based filter and justifies the more complex code.
* Compact filter is significantly faster than a normalized filter.

[ORC-577]: {{ site.jira }}/ORC-577

[HIVE-24458]: {{ site.jira }}/HIVE-24458

[rfilter]: {{ site.repository }}/tree/main/java/bench/core/src/java/org/apache/orc/impl/filter/RowFilter.java

[vfilter]: {{ site.repository }}/tree/main/java/core/src/java/org/apache/orc/impl/filter/VectorFilter.java

[rowvvector]: #RowvsVector

[normalvcompact]: #NormalizationvsCompact

[build]: https://github.com/apache/hive/blob/storage-branch-2.7/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java#L491