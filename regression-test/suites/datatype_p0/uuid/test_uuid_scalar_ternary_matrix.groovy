suite("test_uuid_scalar_ternary_matrix", "p0") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    sql "DROP TABLE IF EXISTS uuid_matrix_scalar_ternary"
    sql """CREATE TABLE uuid_matrix_scalar_ternary (${matrix.schema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_scalar_ternary VALUES ${matrix.values()}"

    sql "DROP TABLE IF EXISTS uuid_matrix_scalar_ternary_notnull"
    sql """CREATE TABLE uuid_matrix_scalar_ternary_notnull (${
           matrix.schema().replace('u UUID,','u UUID NOT NULL,')
                             .replace('v UUID,','v UUID NOT NULL,')
                             .replace('w UUID,','w UUID NOT NULL,')})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_matrix_scalar_ternary_notnull SELECT id,
           IFNULL(u,CAST(REPEAT('0',32) AS UUID)),IFNULL(v,CAST(REPEAT('0',32) AS UUID)),
           IFNULL(w,CAST(REPEAT('0',32) AS UUID)),flag,idx,num,a,a2,flags,m,st FROM uuid_matrix_scalar_ternary"""
    for (boolean nullable : [true,false]) {
        String table = nullable ? 'uuid_matrix_scalar_ternary' : 'uuid_matrix_scalar_ternary_notnull'
        matrix.run(delegate, "ternary_${nullable ? 'nullable' : 'notnull'}", table, ['u','v','w'], { uuidValue,otherUuid,fallbackUuid ->
            [in_values: "${uuidValue} IN (${otherUuid},${fallbackUuid})", not_in_values: "${uuidValue} NOT IN (${otherUuid},${fallbackUuid})",
             between_values: "${uuidValue} BETWEEN ${otherUuid} AND ${fallbackUuid}",
             not_between_values: "${uuidValue} NOT BETWEEN ${otherUuid} AND ${fallbackUuid}",
             simple_case: "CASE ${uuidValue} WHEN ${otherUuid} THEN ${fallbackUuid} ELSE ${uuidValue} END",
             coalesce_value: "COALESCE(${uuidValue},${otherUuid},${fallbackUuid})",
             greatest_value: "GREATEST(${uuidValue},${otherUuid},${fallbackUuid})",
             least_value: "LEAST(${uuidValue},${otherUuid},${fallbackUuid})"]
        })
    }
}
