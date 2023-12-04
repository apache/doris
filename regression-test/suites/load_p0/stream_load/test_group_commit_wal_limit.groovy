
import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_group_commit_wal_limit") {
    def db= "regression_test_load_p0_stream_load"
    def tableName = "test_group_commit_wal_limit"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                k bigint,  
                v string
                )  
                UNIQUE KEY(k)  
                DISTRIBUTED BY HASH (k) BUCKETS 32  
                PROPERTIES(  
                "replication_num" = "1"
            );
    """
    // normal case
    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    strBuilder.append(" -H \"group_commit:true\" -H \"column_separator:,\" " )
    strBuilder.append(" -H \"compress_type:gz\" -H \"format:csv\" " )
    strBuilder.append(" -T " + context.config.dataPath + "/load_p0/stream_load/test_group_commit_wal_limit.csv.gz")
    strBuilder.append(" http://" + context.config.feHttpAddress + "/api/${db}/${tableName}/_stream_load")

    String command = strBuilder.toString()
    logger.info("command is " + command)
    def process = ['bash','-c',command].execute() 
    def code = process.waitFor()
    assertEquals(code, 0)
    def out = process.text
    logger.info("out is " + out )
    assertTrue(out.contains('group_commit'))

    // chunked data case
    strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    strBuilder.append(" -H \"group_commit:true\" -H \"column_separator:,\" " )
    strBuilder.append(" -H \"compress_type:gz\" -H \"format:csv\" " )
    strBuilder.append(" -H \"Content-Length:0\" " )
    strBuilder.append(" -T " + context.config.dataPath + "/load_p0/stream_load/test_group_commit_wal_limit.csv.gz")
    strBuilder.append(" http://" + context.config.feHttpAddress + "/api/${db}/${tableName}/_stream_load")

    command = strBuilder.toString()
    logger.info("command is " + command)
    process = ['bash','-c',command].execute() 
    code = process.waitFor()
    assertEquals(code, 0)
    out = process.text
    logger.info("out is " + out )
    assertTrue(out.contains('[INTERNAL_ERROR]'))

    // too lagre data case 1TB
    strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    strBuilder.append(" -H \"group_commit:true\" -H \"column_separator:,\" " )
    strBuilder.append(" -H \"compress_type:gz\" -H \"format:csv\" " )
    strBuilder.append(" -H \"Content-Length:1099511627776\" " )
    strBuilder.append(" -T " + context.config.dataPath + "/load_p0/stream_load/test_group_commit_wal_limit.csv.gz")
    strBuilder.append(" http://" + context.config.feHttpAddress + "/api/${db}/${tableName}/_stream_load")

    command = strBuilder.toString()
    logger.info("command is " + command)
    process = ['bash','-c',command].execute() 
    code = process.waitFor()
    assertEquals(code, 0)
    out = process.text
    logger.info("out is " + out )
    assertTrue(out.contains('[INTERNAL_ERROR]'))
}
