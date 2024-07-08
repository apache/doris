import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_recycler_with_internal_copy") {
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId
    def cloudUniqueId = context.config.cloudUniqueId
    def filePath = "${context.config.dataPath}/cloud/copy_into/internal_customer.csv"

    def upload = { Set<String> fileNames ->
        long timestamp = System.currentTimeMillis()
        for (int i = 0; i < 5; i++) {
            StringBuilder strBuilder = new StringBuilder()
            strBuilder.append("""curl -u """ + context.config.feCloudHttpUser + ":" + context.config.feCloudHttpPassword)
            def fileName = "test_recycler_expired_stage_objects_" + (timestamp + i)
            strBuilder.append(""" -H fileName:""" + fileName);
            strBuilder.append(""" -T """ + filePath)
            strBuilder.append(""" -L http://""" + context.config.feCloudHttpAddress + """/copy/upload""")

            String command = strBuilder.toString()
            logger.info("upload command=" + command)
            def process = command.toString().execute()
            def code = process.waitFor()
            def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            def out = process.getText()
            logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            fileNames.add(fileName)
        }
    }

    // upload files
    Set<String> fileNames1 = new HashSet<>()
    upload(fileNames1)

    // 180 seconds, must set internal_stage_objects_expire_time_second in recycler
    Thread.sleep(180 * 1000)

    // upload files again
    Set<String> fileNames2 = new HashSet<>()
    upload(fileNames2)

    logger.info("expected non exist files: " + fileNames1)
    logger.info("expected exist files: " + fileNames2)

    int retry = 10
    boolean success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000)
        if (checkRecycleExpiredStageObjects(token, instanceId, cloudUniqueId, fileNames1, fileNames2)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
