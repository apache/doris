import java.text.SimpleDateFormat;

suite("test_recycler_with_dynamic_partition") {
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId
    def tableName = 'test_recycler_with_dynamic_partition'
    // todo: test dynamic partition
    sql "drop table if exists ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            k1 date NOT NULL,
            k2 varchar(20) NOT NULL,
            k3 int sum NOT NULL
        )
        AGGREGATE KEY(k1,k2) 
        PARTITION BY RANGE(k1) ( ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 3 
        PROPERTIES (  
            "dynamic_partition.enable"="true", 
            "dynamic_partition.end"="3", 
            "dynamic_partition.buckets"="4", 
            "dynamic_partition.start"="-3", 
            "dynamic_partition.prefix"="p", 
            "dynamic_partition.time_unit"="DAY", 
            "dynamic_partition.create_history_partition"="true"
        )
        """

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    for (int i = -3; i++; i <= 3) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, i);
        String dateStr = sdf.format(calendar.getTime());
        sql """insert into ${tableName} values ('${dateStr}', 'aaa', 1);"""
    }

    qt_sql """ select count(*) from ${tableName};"""

    String[][] tabletInfoList = sql """ show tablets from ${tableName}; """
    logger.debug("tabletInfoList:${tabletInfoList}")

    HashSet<String> tabletIdSet= new HashSet<String>()
    for (tabletInfo : tabletInfoList) {
        tabletIdSet.add(tabletInfo[0])
    }
    logger.info("tabletIdSet:${tabletIdSet}")

    // drop table
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""

    int retry = 15
    boolean success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000) // 20s
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
