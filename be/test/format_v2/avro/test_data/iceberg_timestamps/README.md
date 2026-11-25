# Iceberg Avro timestamp fixtures

`GenerateIcebergTimestamps.java` creates these files with Apache Iceberg 1.12.0
`Avro.write`, its `TypeToSchema` conversion, and `GenericAvroWriter`. The writer
accepts Avro indexed records containing encoded integer timestamp values. Iceberg
owns the schema, field IDs, OCF framing, and `adjust-to-utc` annotations.

| File | Iceberg type | Avro logical type | `adjust-to-utc` |
| --- | --- | --- | --- |
| `iceberg_civil_micros.avro` | `TimestampType.withoutZone()` | `timestamp-micros` | `false` |
| `iceberg_instant_micros.avro` | `TimestampType.withZone()` | `timestamp-micros` | `true` |
| `iceberg_civil_nanos.avro` | `TimestampNanoType.withoutZone()` | `timestamp-nanos` | `false` |
| `iceberg_instant_nanos.avro` | `TimestampNanoType.withZone()` | `timestamp-nanos` | `true` |
| `iceberg_position_deletes.avro` | position-delete shape with a civil-nanos `row.clock` | `timestamp-nanos` in the row | `false` |

Iceberg represents civil timestamps with the standard timestamp logical annotation
and an explicit false adjustment flag. The native reader must preserve their wall
clock values in a non-UTC query timezone. Instant micros use query-timezone
conversion. The instant-nanos file is an independent unsupported-type control.
Keeping it separate lets the positive civil cases initialize their complete schemas.
Iceberg nano timestamp table types require format version 3; the native nano data
case uses that table version. These standalone OCFs contain no catalog or snapshot.

The actual producer contracts are in the pinned
[TypeToSchema](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/avro/TypeToSchema.java),
[Avro appender](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/avro/Avro.java),
and [GenericAvroWriter](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/core/src/main/java/org/apache/iceberg/avro/GenericAvroWriter.java).

## Rows and field IDs

The four data schemas have required `id` INT (field ID 1), optional `clock`
(field ID 2), and optional `payload` STRUCT (field ID 3) with optional `clock`
(field ID 4). IDs 1 through 8 identify rows in file order.

The root clock values for rows 1 through 7 are:

```text
micros: 0, 1, -1, 123456, -123456, 1730611800123456, 1730615400654321
nanos:  0, 1, -1, 123456789, -123456789, 1730611800123456789, 1730615400654321987
```

Row 8 has a null root clock. Nested values deliberately differ from some root values:

| Row ID | Payload |
| --- | --- |
| 1 | clock equal to root row 1 |
| 2 | null struct |
| 3 | present struct with null clock |
| 4 | clock equal to root row 5 |
| 5 | clock equal to root row 4 |
| 6 | clock equal to root row 7 |
| 7 | clock equal to root row 6 |
| 8 | null struct |

For civil values, both UTC and `America/New_York` retain the same epoch-relative
wall-clock components. For example, encoded microseconds -1 means
`1969-12-31 23:59:59.999999`; encoded nanoseconds -1 means
`1969-12-31 23:59:59.999999999`. An instant-micros value of 0 displays as
`1970-01-01 00:00:00.000000` in UTC and `1969-12-31 19:00:00.000000` in New York.
Rows 6 and 7 also exercise the two UTC instants around New York's 2024 fall transition.

The position-delete-shaped file has required `file_path` STRING (ID 2147483546),
required `pos` LONG (ID 2147483545), and optional `row` STRUCT (ID 2147483544)
containing optional civil-nanos `clock` (ID 2). Its records are:

```text
file_path    pos    row.clock
"data.avro"   0      0
"data.avro"   2     -1
```

This exercises full schema initialization when a delete reader projects only path
and position: the unprojected row still carries a genuine Iceberg civil annotation.

## Regenerate

Use JDK 17 and a populated local Maven repository. The recipe uses the Doris
repository's Avro/Jackson pins and cached Iceberg runtime dependencies. It builds
only this standalone producer helper.

```bash
maven_repository=${MAVEN_REPOSITORY:-"$HOME/.m2/repository"}
fixture_jars=(
  org/apache/iceberg/iceberg-api/1.12.0/iceberg-api-1.12.0.jar
  org/apache/iceberg/iceberg-common/1.12.0/iceberg-common-1.12.0.jar
  org/apache/iceberg/iceberg-core/1.12.0/iceberg-core-1.12.0.jar
  org/apache/iceberg/iceberg-bundled-guava/1.12.0/iceberg-bundled-guava-1.12.0.jar
  org/apache/avro/avro/1.12.1/avro-1.12.1.jar
  com/fasterxml/jackson/core/jackson-core/2.21.8/jackson-core-2.21.8.jar
  com/fasterxml/jackson/core/jackson-databind/2.21.8/jackson-databind-2.21.8.jar
  com/fasterxml/jackson/core/jackson-annotations/2.21/jackson-annotations-2.21.jar
  org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar
  com/github/ben-manes/caffeine/caffeine/3.2.4/caffeine-3.2.4.jar
  org/apache/commons/commons-compress/1.27.1/commons-compress-1.27.1.jar
)
fixture_classpath=""
for fixture_jar in "${fixture_jars[@]}"; do
  test -r "$maven_repository/$fixture_jar"
  fixture_classpath="${fixture_classpath:+$fixture_classpath:}$maven_repository/$fixture_jar"
done
generation_dir=$(mktemp -d ./generated-timestamps.XXXXXX)
mkdir "$generation_dir/classes"
javac --release 17 -cp "$fixture_classpath" -d "$generation_dir/classes" GenerateIcebergTimestamps.java
java -XX:ActiveProcessorCount=1 -Xmx256m -cp "$generation_dir/classes:$fixture_classpath" \
  GenerateIcebergTimestamps "$generation_dir/output"
```

The generator verifies field IDs and the written adjustment flags, then reads every
row through Iceberg's `GenericAvroReader` and checks its encoded values. It emits
five OCFs plus a local `manifest.json`/`manifest.sha256` containing schema snapshots,
value/display oracles, class/JAR identities, and actual content hashes. Retain the
manifest as generation evidence and copy the five verified OCFs when refreshing
these fixtures. Producer-generated sync markers may change file hashes on
regeneration; schema and row oracles remain stable. The generator refuses to
replace an existing fixture.
