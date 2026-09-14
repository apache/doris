suite("test_uuid_array_element_matrix", "p0") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    sql "DROP TABLE IF EXISTS uuid_matrix_array_element"
    sql """CREATE TABLE uuid_matrix_array_element (${matrix.schema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_matrix_array_element VALUES ${matrix.values()}"

    matrix.run(delegate, 'element', 'uuid_matrix_array_element', ['a','u'], { arrayValue,uuidValue ->
        [contains_value: "ARRAY_CONTAINS(${arrayValue},${uuidValue})", position_value: "ARRAY_POSITION(${arrayValue},${uuidValue})",
         removed: "ARRAY_REMOVE(${arrayValue},${uuidValue})", pushed_back: "ARRAY_PUSHBACK(${arrayValue},${uuidValue})",
         pushed_front: "ARRAY_PUSHFRONT(${arrayValue},${uuidValue})", appended: "ARRAY_APPEND(${arrayValue},${uuidValue})",
         count_equal: "COUNTEQUAL(${arrayValue},${uuidValue})"]
    })
    matrix.run(delegate, 'capture', 'uuid_matrix_array_element', ['a','u'], { arrayValue,uuidValue ->
        [mapped: "ARRAY_MAP(element -> COALESCE(element,${uuidValue}),${arrayValue})",
         filtered: "ARRAY_FILTER(element -> element <=> ${uuidValue},${arrayValue})",
         count_matches: "ARRAY_COUNT(element -> element <=> ${uuidValue},${arrayValue})",
         exists_match: "ARRAY_EXISTS(element -> element <=> ${uuidValue},${arrayValue})",
         any_match: "ARRAY_MATCH_ANY(element -> element <=> ${uuidValue},${arrayValue})",
         all_match: "ARRAY_MATCH_ALL(element -> element <=> ${uuidValue},${arrayValue})",
         first_value: "ARRAY_FIRST(element -> element <=> ${uuidValue},${arrayValue})",
         last_value: "ARRAY_LAST(element -> element <=> ${uuidValue},${arrayValue})",
         first_index: "ARRAY_FIRST_INDEX(element -> element <=> ${uuidValue},${arrayValue})",
         last_index: "ARRAY_LAST_INDEX(element -> element <=> ${uuidValue},${arrayValue})",
         filtered_unknown: "ARRAY_FILTER(element -> element = ${uuidValue},${arrayValue})",
         count_unknown: "ARRAY_COUNT(element -> element = ${uuidValue},${arrayValue})",
         exists_unknown: "ARRAY_EXISTS(element -> element = ${uuidValue},${arrayValue})",
         any_unknown: "ARRAY_MATCH_ANY(element -> element = ${uuidValue},${arrayValue})",
         all_unknown: "ARRAY_MATCH_ALL(element -> element = ${uuidValue},${arrayValue})"]
    })
    matrix.run(delegate, 'index', 'uuid_matrix_array_element', ['a','idx'], { arrayValue,offsetValue ->
        [element_value: "ELEMENT_AT(${arrayValue},${offsetValue})", subscript_value: "(${arrayValue})[${offsetValue}]",
         sliced: "ARRAY_SLICE(${arrayValue},${offsetValue})"]
    })
    matrix.run(delegate, 'slice', 'uuid_matrix_array_element', ['a','idx','num'], { arrayValue,offsetValue,lengthValue ->
        [sliced: "ARRAY_SLICE(${arrayValue},${offsetValue},${lengthValue})"]
    })
    matrix.run(delegate, 'repeat', 'uuid_matrix_array_element', ['u','num'], { uuidValue,repeatCount ->
        [repeated: "ARRAY_REPEAT(${uuidValue},${repeatCount})", with_constant: "ARRAY_WITH_CONSTANT(${repeatCount},${uuidValue})"]
    })
    matrix.run(delegate, 'constructor', 'uuid_matrix_array_element', ['u','v','w'], { uuidValue,otherUuid,fallbackUuid ->
        [array_value: "ARRAY(${uuidValue},${otherUuid},${fallbackUuid})"]
    })
}
