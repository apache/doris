def createTable = { tableName ->
    sql """
        create table ${tableName}
        (id int)
        distributed by hash(id)
        properties
        (
          "replication_num"="1"
        )
        """
}

def tableName = "test_events_table1"
createTable(tableName)

// lazy drop table when execute this suite finished
onFinish {
    try_sql "drop table if exists ${tableName}"
}



// all event: success, fail, finish
// and you can listen event multiple times

onSuccess {
    try_sql "drop table if exists ${tableName}"
}

onSuccess {
    try_sql "drop table if exists ${tableName}_not_exist"
}

onFail {
    try_sql "drop table if exists ${tableName}"
}

onFail {
    try_sql "drop table if exists ${tableName}_not_exist"
}