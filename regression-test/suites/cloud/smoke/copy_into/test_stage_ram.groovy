import groovy.json.JsonOutput

suite("test_stage_ram") {
    def instance_id = context.config.instanceId
    def ms_token = context.config.metaServiceToken

    def stageEndpoint = context.config.stageIamEndpoint
    def region = context.config.stageIamRegion
    def bucket = context.config.stageIamBucket
    def policyName = context.config.stageIamPolicy

    def roleName = context.config.stageIamRole
    def arn = context.config.stageIamArn

    def ak = context.config.stageIamAk
    def sk = context.config.stageIamSk
    def userId = context.config.stageIamUserId

    String provider = "${getS3Provider()}"
    String stageName = "test_stage_iam"

    def get_instance = { requestBody, checkFunc ->
        httpTest {
            endpoint context.config.metaServiceHttpAddress
            uri "/MetaService/http/get_instance?token=${ms_token}&instance_id=${instance_id}"
            body requestBody
            check checkFunc
        }
    }

    // get instance
    def jsonOutput = new JsonOutput()
    def requestBody = jsonOutput.toJson([])
    def instance_info = null
    get_instance.call(requestBody) {
        respCode, body ->
            log.info("http cli result: ${body} ${respCode}".toString())
            instance_info = parseJson(body)
            assertTrue(instance_info.code.equalsIgnoreCase("OK"))
    }
    def instanceIamUserId = instance_info.result.iam_user.user_id
    log.info("instance iam user id: " + instanceIamUserId)
    assertFalse(instanceIamUserId.isEmpty(), "instance iam user should not be empty")

    try {
        if (provider.equals("oss") || provider.equals("obs")) {
            // use role name and arn from conf file
        } else {
            // create role and attach policy (ANNT: policy must be created)
            def roleAndArn = createRole(provider, region, ak, sk, instanceIamUserId, instance_id, policyName, userId)
            roleName = roleAndArn[0]
            arn = roleAndArn[1]
            log.info("succeed to create role, roleName: ${roleName}, arn: ${arn}")
        }

        // create stage
        sql """ drop stage if exists ${stageName} """
        def retry = 10
        for (def i= 0; i < retry; ++i) {
            try {
                sql """
                create stage ${stageName} properties (
                    'endpoint' = '${stageEndpoint}' ,
                    'region' = '${region}' ,
                    'bucket' = '${bucket}' ,
                    'provider' = '${provider}',
                    'access_type' = 'iam',
                    'role_name'='${roleName}',
                    'arn'='${arn}'
                );
                """
                break
            } catch (Exception e) {
                if (i == retry - 1) {
                    throw e
                }
                logger.info("create stage catch exception: " + e.getMessage() + ", retry: " + i)
                // attached policy need some time to be valid
                Thread.sleep(6000)
            }
        }

        // TODO load data

    } catch (Exception e) {
        assertTrue(false, "test_stage_ram catch exception: " + e.getMessage())
    } finally {
        try {
            sql """ drop stage if exists ${stageName} """
        } catch (Exception e) {
            // ignore
        }
        try {
            // detach policy and delete role, do nothing for oss and s3
            deleteRole(provider, region, ak, sk, instance_id, policyName)
        } catch (Exception e) {
            logger.error("failed delete role: " + e.getMessage())
        }
    }
}

