suite("test_pythonudf_uuid_binary") {
    def runtimeVersion = getPythonUdfRuntimeVersion()
    sql "DROP TABLE IF EXISTS python_uuid_binary"
    sql """CREATE TABLE python_uuid_binary (
        id INT, u UUID, a ARRAY<UUID>, m MAP<UUID,UUID>,
        s STRUCT<u:UUID,items:ARRAY<UUID>>, nested ARRAY<MAP<UUID,UUID>>
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO python_uuid_binary VALUES
        (1, '00112233-4455-6677-8899-aabbccddeeff',
         ['00112233-4455-6677-8899-aabbccddeeff', NULL],
         {'00112233-4455-6677-8899-aabbccddeeff': NULL},
         {'00112233-4455-6677-8899-aabbccddeeff', [NULL]},
         [{'00112233-4455-6677-8899-aabbccddeeff': 'ffffffff-ffff-ffff-ffff-ffffffffffff'}, NULL, {}]),
        (2, '80000000-0000-0000-0000-000000000000', [], {}, {NULL, []}, []),
        (3, 'ffffffff-ffff-ffff-ffff-ffffffffffff', NULL, NULL, NULL, NULL),
        (4, '00000000-0000-0000-0000-000000000000', [], {}, {NULL, NULL}, []),
        (5, NULL, NULL, NULL, NULL, NULL)"""
    def identityTypes = [
        ["py_uuid_binary_identity", "UUID"],
        ["py_uuid_binary_array", "ARRAY<UUID>"],
        ["py_uuid_binary_map", "MAP<UUID,UUID>"],
        ["py_uuid_binary_struct", "STRUCT<u:UUID,items:ARRAY<UUID>>"],
        ["py_uuid_binary_nested", "ARRAY<MAP<UUID,UUID>>"]
    ]
    identityTypes.each { definition ->
        sql "DROP FUNCTION IF EXISTS ${definition[0]}(${definition[1]})"
        sql """CREATE FUNCTION ${definition[0]}(${definition[1]})
        RETURNS ${definition[1]}
        PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="${runtimeVersion}")
        AS \$\$
import uuid
def validate(value):
    if value is None or isinstance(value, uuid.UUID):
        return
    if isinstance(value, list):
        for item in value:
            validate(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            validate(item)
        return
    raise TypeError("expected native UUID, got " + type(value).__name__)
def evaluate(value):
    validate(value)
    return value
\$\$"""
    }
    qt_scalar """SELECT id, py_uuid_binary_identity(u), py_uuid_binary_array(a),
        py_uuid_binary_map(m), py_uuid_binary_struct(s), py_uuid_binary_nested(nested)
        FROM python_uuid_binary ORDER BY id"""
    qt_empty "SELECT py_uuid_binary_identity(u) FROM python_uuid_binary WHERE id < 0 ORDER BY id"
    qt_constant """SELECT py_uuid_binary_identity(CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID))"""

    sql "DROP FUNCTION IF EXISTS py_uuid_binary_list(UUID)"
    sql """CREATE FUNCTION py_uuid_binary_list(UUID) RETURNS UUID
        PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="${runtimeVersion}")
        AS \$\$
import uuid
def evaluate(values: list):
    for value in values:
        if value is not None and not isinstance(value, uuid.UUID):
            raise TypeError("expected native UUID")
    return values
\$\$"""
    sql "DROP FUNCTION IF EXISTS py_uuid_binary_series(UUID)"
    sql """CREATE FUNCTION py_uuid_binary_series(UUID) RETURNS UUID
        PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="${runtimeVersion}")
        AS \$\$
import pandas as pd
import uuid
def evaluate(values: pd.Series):
    for value in values:
        if value is not None and not isinstance(value, uuid.UUID):
            raise TypeError("expected UUID objects in pandas")
    return values
\$\$"""
    sql "DROP FUNCTION IF EXISTS py_uuid_binary_vector_array(ARRAY<UUID>)"
    sql """CREATE FUNCTION py_uuid_binary_vector_array(ARRAY<UUID>) RETURNS ARRAY<UUID>
        PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="${runtimeVersion}")
        AS \$\$
import uuid
def evaluate(rows: list):
    for row in rows:
        if row is not None:
            for value in row:
                if value is not None and not isinstance(value, uuid.UUID):
                    raise TypeError("expected nested native UUID")
    return rows
\$\$"""
    qt_vectors """SELECT id, py_uuid_binary_list(u), py_uuid_binary_series(u),
        py_uuid_binary_vector_array(a) FROM python_uuid_binary ORDER BY id"""
    sql "DROP FUNCTION IF EXISTS py_uuid_binary_broadcast(UUID)"
    sql """CREATE FUNCTION py_uuid_binary_broadcast(UUID) RETURNS UUID
        PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="${runtimeVersion}")
        AS \$\$
import uuid
def evaluate(values: list):
    return uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
\$\$"""
    qt_broadcast "SELECT id, py_uuid_binary_broadcast(u) FROM python_uuid_binary ORDER BY id"
    sql "DROP FUNCTION IF EXISTS py_uuid_binary_string(STRING)"
    sql """CREATE FUNCTION py_uuid_binary_string(STRING) RETURNS STRING
        PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="${runtimeVersion}")
        AS \$\$
def evaluate(value):
    if value is not None and not isinstance(value, str):
        raise TypeError("expected unchanged STRING")
    return value
\$\$"""
    qt_strings """SELECT id, py_uuid_binary_string(CAST(u AS STRING))
        FROM python_uuid_binary ORDER BY id"""
    sql "DROP TABLE IF EXISTS python_uuid_binary_batches"
    sql """CREATE TABLE python_uuid_binary_batches (id BIGINT, u UUID)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO python_uuid_binary_batches
        SELECT number, CAST(MD5(CAST(number AS STRING)) AS UUID) FROM numbers("number"="4099")"""
    sql "INSERT INTO python_uuid_binary_batches VALUES (4100, uuid_v7()), (4101, NULL)"
    qt_batches """SELECT COUNT(*), SUM(IF(u <=> py_uuid_binary_identity(u), 0, 1)),
        SUM(IF(u <=> py_uuid_binary_list(u), 0, 1)),
        SUM(IF(u <=> py_uuid_binary_series(u), 0, 1)) FROM python_uuid_binary_batches"""
    ["value", "values: list"].eachWithIndex { parameter, index ->
        sql "DROP FUNCTION IF EXISTS py_uuid_binary_not_nullable_${index}(UUID)"
        sql """CREATE FUNCTION py_uuid_binary_not_nullable_${index}(UUID) RETURNS UUID
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "always_nullable"="false",
                "runtime_version"="${runtimeVersion}") AS \$\$
def evaluate(${parameter}):
    return None
\$\$"""
        test {
            sql "SELECT py_uuid_binary_not_nullable_${index}(u) FROM python_uuid_binary WHERE id = 1"
            exception "but the return type is not nullable"
        }
    }
    ["str(value)", "value.bytes", "value.int"].eachWithIndex { expression, index ->
        def functionName = "py_uuid_binary_invalid_${index}"
        sql "DROP FUNCTION IF EXISTS ${functionName}(UUID)"
        sql """CREATE FUNCTION ${functionName}(UUID) RETURNS UUID
            PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="${runtimeVersion}")
            AS \$\$
def evaluate(value):
    return ${expression}
\$\$"""
        test {
            sql "SELECT ${functionName}(u) FROM python_uuid_binary WHERE id = 1"
            exception "UUID return value must be uuid.UUID"
        }
    }
    sql "DROP FUNCTION IF EXISTS py_uuid_binary_min(UUID)"
    sql """CREATE AGGREGATE FUNCTION py_uuid_binary_min(UUID) RETURNS UUID
        PROPERTIES("type"="PYTHON_UDF", "symbol"="UuidMin", "runtime_version"="${runtimeVersion}")
        AS \$\$
import uuid
class UuidMin:
    def __init__(self):
        self.value = None
    def accumulate(self, value):
        if value is not None:
            if not isinstance(value, uuid.UUID):
                raise TypeError("expected native UUID")
            if self.value is None or value.int < self.value.int:
                self.value = value
    def merge(self, other_state):
        self.accumulate(other_state)
    def finish(self):
        return self.value
    @property
    def aggregate_state(self):
        return self.value
\$\$"""
    qt_udaf """SELECT id % 2 AS bucket, py_uuid_binary_min(u)
        FROM python_uuid_binary GROUP BY id % 2 ORDER BY bucket"""
    qt_udaf_null "SELECT py_uuid_binary_min(u) FROM python_uuid_binary WHERE id = 5"
    sql "DROP FUNCTION IF EXISTS py_uuid_binary_rows(UUID)"
    sql """CREATE TABLES FUNCTION py_uuid_binary_rows(UUID) RETURNS ARRAY<UUID>
        PROPERTIES("type"="PYTHON_UDF", "symbol"="evaluate", "runtime_version"="${runtimeVersion}")
        AS \$\$
import uuid
def evaluate(value):
    if value is not None and not isinstance(value, uuid.UUID):
        raise TypeError("expected native UUID")
    yield (value,)
    yield (uuid.UUID(int=0),)
\$\$"""
    qt_udtf """SELECT id, result FROM python_uuid_binary
        LATERAL VIEW py_uuid_binary_rows(u) expanded AS result ORDER BY id, result"""
}
