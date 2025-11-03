// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
import groovy.json.JsonOutput
import org.apache.doris.regression.suite.Suite
import org.codehaus.groovy.runtime.IOGroovyMethods
import java.util.Base64

import com.aliyuncs.DefaultAcsClient
import com.aliyuncs.IAcsClient
import com.aliyuncs.exceptions.ClientException
import com.aliyuncs.profile.DefaultProfile
import com.aliyuncs.ram.model.v20150501.*

import com.tencentcloudapi.common.Credential
import com.tencentcloudapi.cam.v20190116.CamClient

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.iam.model.AttachRolePolicyRequest
import software.amazon.awssdk.services.iam.model.DetachRolePolicyRequest
import software.amazon.awssdk.services.iam.model.EntityAlreadyExistsException;

Suite.metaClass.getRoleName = { String instanceId ->
    return "smoke-test-" + instanceId.replaceAll("_", "-")
}
logger.info("Added 'getRoleName' function to Suite")

Suite.metaClass.getExternalId = { String instanceId ->
    return Base64.getEncoder().encodeToString(instanceId.getBytes("UTF-8"));
}
logger.info("Added 'getExternalId' function to Suite")

// oss: return arn
// cos: return empty means role already exists
// obs: (return agency id)
// s3:  return arn
Suite.metaClass.getRole = { String provider, String region, String ak, String sk, String instanceId ->
    Suite suite = delegate as Suite
    suite.getLogger().info("cloud_stage_ram plugin: suiteName: ${suite.name}, getRole, provider: ${provider}, instance: ${instanceId}, region:${region}".toString())
    def roleName = getRoleName(instanceId)

    if (provider.equalsIgnoreCase("oss")) {
        DefaultProfile profile = DefaultProfile.getProfile(region, ak, sk);
        IAcsClient client = new DefaultAcsClient(profile);
        GetRoleRequest getRoleRequest = new GetRoleRequest();
        getRoleRequest.setRoleName(roleName)
        try {
            GetRoleResponse response = client.getAcsResponse(getRoleRequest)
            return response.getRole().getArn()
        } catch (Exception e) {
            return null
        }
    } else if (provider.equalsIgnoreCase("cos")) {
        Credential cred = new Credential(ak, sk)
        CamClient client = new CamClient(cred, "")
        com.tencentcloudapi.cam.v20190116.models.GetRoleRequest getRoleRequest = new com.tencentcloudapi.cam.v20190116.models.GetRoleRequest()
        getRoleRequest.setRoleName(roleName)
        try {
            com.tencentcloudapi.cam.v20190116.models.GetRoleResponse getRoleResponse = client.GetRole(getRoleRequest)
            // ANNT: txcloud does not return arn
            return ""
        } catch (Exception e) {
            return null
        }
    } else if (provider.equalsIgnoreCase("obs")) {
        // do nothing
    } else if (provider.equalsIgnoreCase("s3")) {
        try {
            AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(ak, sk)
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(basicCredentials)
            software.amazon.awssdk.services.iam.IamClient iam = software.amazon.awssdk.services.iam.IamClient.builder()
                    .region(Region.of(region)).credentialsProvider(scp).build()
            software.amazon.awssdk.services.iam.model.GetRoleRequest roleRequest = software.amazon.awssdk.services.iam.model.GetRoleRequest.builder()
                    .roleName(roleName).build()
            software.amazon.awssdk.services.iam.model.GetRoleResponse response = iam.getRole(roleRequest)
            return response.role().arn()
        } catch (Exception e) {
            return null
        }
    }
    throw new Exception("Unsupported provider [" + provider + "] for get role")
}
logger.info("Added 'getRole' function to Suite")

// s3: policyName is policy arn
Suite.metaClass.attachPolicy = { String provider, String region, String ak, String sk, String roleName, String policyName ->
    Suite suite = delegate as Suite
    suite.getLogger().info("cloud_stage_ram plugin: suiteName: ${suite.name}, attachPolicy, provider: ${provider}, region: ${region}, roleName: ${roleName}, policyName: ${policyName}".toString())

    if (provider.equalsIgnoreCase("oss")) {
        DefaultProfile profile = DefaultProfile.getProfile(region, ak, sk)
        IAcsClient client = new DefaultAcsClient(profile)
        AttachPolicyToRoleRequest attachPolicyToRoleRequest = new AttachPolicyToRoleRequest();
        attachPolicyToRoleRequest.setPolicyType("Custom")
        attachPolicyToRoleRequest.setPolicyName(policyName)
        attachPolicyToRoleRequest.setRoleName(roleName)
        try {
            AttachPolicyToRoleResponse response = client.getAcsResponse(attachPolicyToRoleRequest)
        } catch (ClientException e) {
            if (!e.getErrCode().equals("EntityAlreadyExists.Role.Policy")) {
                throw e
            }
        }
    } else if (provider.equalsIgnoreCase("cos")) {
        // cos does not throw exception if policy is already attached
        Credential cred = new Credential(ak, sk)
        CamClient client = new CamClient(cred, "")
        com.tencentcloudapi.cam.v20190116.models.AttachRolePolicyRequest attachRolePolicyRequest = new com.tencentcloudapi.cam.v20190116.models.AttachRolePolicyRequest()
        attachRolePolicyRequest.setAttachRoleName(roleName)
        attachRolePolicyRequest.setPolicyName(policyName)
        com.tencentcloudapi.cam.v20190116.models.AttachRolePolicyResponse attachRolePolicyResponse = client.AttachRolePolicy(attachRolePolicyRequest)
    } else if (provider.equalsIgnoreCase("s3")) {
        try {
            AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(ak, sk)
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(basicCredentials)
            software.amazon.awssdk.services.iam.IamClient iam = software.amazon.awssdk.services.iam.IamClient.builder()
                    .region(Region.of(region)).credentialsProvider(scp).build()
            AttachRolePolicyRequest attachRequest = AttachRolePolicyRequest.builder()
                    .roleName(roleName).policyArn(policyName).build()
            iam.attachRolePolicy(attachRequest)
        } catch (EntityAlreadyExistsException e) {
            // policy already detached, ignore
        }
    } else {
        throw new Exception("Unsupported provider [" + provider + "] for attach policy")
    }
}
logger.info("Added 'attachPolicy' function to Suite")

Suite.metaClass.createRole = { String provider, String region, String ak, String sk, String instanceRamUserId, String instanceId, String policy, String userId ->
    Suite suite = delegate as Suite
    suite.getLogger().info("cloud_stage_ram plugin: suiteName: ${suite.name}, createRole, provider: ${provider}, region: ${region}, instance: ${instanceId}, region: ${region}".toString())
    def roleName = getRoleName(instanceId)
    def roleDescription = "冒烟测试: " + instanceId
    def externalId = getExternalId(instanceId)

    if (provider.equalsIgnoreCase("oss")) {
        // check if role exists
        def arn = getRole(provider, region, ak, sk, instanceId)
        if (arn != null) {
            // attach policy
            attachPolicy(provider, region, ak, sk, roleName, policy)
            suite.getLogger().info("Role [${roleName}] already exist, arn [${arn}], skip create.")
            return new String[]{roleName, arn}
        }

        // role does not exist, create role
        DefaultProfile profile = DefaultProfile.getProfile(region, ak, sk)
        IAcsClient client = new DefaultAcsClient(profile)
        CreateRoleRequest request = new CreateRoleRequest()
        request.setRoleName(roleName)
        /**
         * {
         *   "Statement": [
         *     {
         *       "Action": "sts:AssumeRole",
         *       "Effect": "Allow",
         *       "Principal": {
         *         "RAM": [
         *           "acs:ram::1276155707910852:root"
         *         ]
         *       }
         *     }
         *   ],
         *   "Version": "1"
         * }
         */
        request.setAssumeRolePolicyDocument("{\n" +
                "  \"Statement\": [\n" +
                "    {\n" +
                "      \"Action\": \"sts:AssumeRole\",\n" +
                "      \"Effect\": \"Allow\",\n" +
                "      \"Principal\": {\n" +
                "        \"RAM\": [\n" +
                "          \"acs:ram::" + instanceRamUserId + ":root\"\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  \"Version\": \"1\"\n" +
                "}");
        request.setDescription(roleDescription)
        // request.setMaxSessionDuration(3600)
        CreateRoleResponse response = client.getAcsResponse(request)

        // attach policy
        attachPolicy(provider, region, ak, sk, roleName, policy)
        return new String[]{roleName, response.getRole().getArn()}
    } else if (provider.equals("cos")) {
        // check if role exists
        def arn = getRole(provider, region, ak, sk, instanceId)
        if (arn != null) {
            arn = "qcs::cam::uin/" + userId + ":roleName/" + roleName
            suite.getLogger().info("Role [${roleName}] already exist, arn [${arn}], skip create.")
            // attach policy
            attachPolicy(provider, region, ak, sk, roleName, policy)
            return new String[]{roleName, arn}
        }

        Credential cred = new Credential(ak, sk)
        /*HttpProfile httpProfile = new HttpProfile()
        httpProfile.setEndpoint("cam.tencentcloudapi.com")
        ClientProfile clientProfile = new ClientProfile()
        clientProfile.setHttpProfile(httpProfile)
        CamClient client = new CamClient(cred, region, clientProfile)*/
        CamClient client = new CamClient(cred, "")
        com.tencentcloudapi.cam.v20190116.models.CreateRoleRequest req = new com.tencentcloudapi.cam.v20190116.models.CreateRoleRequest()
        req.setRoleName(roleName)
        /**
         * {
         *   "Statement": [
         *     {
         *       "Action": "name/sts:AssumeRole",
         *       "Effect": "Allow",
         *       "Principal": {
         *         "qcs": [
         *             "qcs::cam::uin/100029159234:root"
         *         ]
         *       },
         *       "condition": {
         *          "string_equal": {
         *              "sts:external_id": "aW5zdGFuY2VfbWVpeWlfZGV2"
         *          }
         *       }
         *     }
         *   ],
         *   "Version": "2.0"
         * }
         */
        String policyDocument = "{\n" +
                "  \"Statement\": [\n" +
                "    {\n" +
                "      \"Action\": \"name/sts:AssumeRole\",\n" +
                "      \"Effect\": \"Allow\",\n" +
                "      \"Principal\": {\n" +
                "        \"qcs\": [\n" +
                "            \"qcs::cam::uin/" + instanceRamUserId + ":root\"\n" +
                "        ]\n" +
                "      },\n" +
                "      \"condition\": {\n" +
                "        \"string_equal\": {\n" +
                "            \"sts:external_id\": \"" + externalId + "\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  \"Version\": \"2.0\"\n" +
                "}";
        req.setPolicyDocument(policyDocument)
        req.setDescription(roleDescription)
        com.tencentcloudapi.cam.v20190116.models.CreateRoleResponse resp = client.CreateRole(req)

        // attach policy
        attachPolicy(provider, region, ak, sk, roleName, policy)

        // ANNT: txcloud does not return arn
        return new String[]{roleName, "qcs::cam::uin/" + userId + ":roleName/" + roleName}
    } else if (provider.equals("s3")) {
        // s3 does not support chinese characters
        roleDescription = "smoke-test: " + instanceId
        // check if role exists
        def arn = getRole(provider, region, ak, sk, instanceId)
        if (arn != null) {
            // attach policy
            attachPolicy(provider, region, ak, sk, roleName, policy)
            suite.getLogger().info("Role [${roleName}] already exist, arn [${arn}], skip create.")
            return new String[]{roleName, arn}
        }

        // create role
        AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(ak, sk)
        StaticCredentialsProvider scp = StaticCredentialsProvider.create(basicCredentials)
        software.amazon.awssdk.services.iam.IamClient iam = software.amazon.awssdk.services.iam.IamClient.builder()
                .region(Region.of(region)).credentialsProvider(scp).build()
        /**
         * {
         *     "Version": "2012-10-17",
         *     "Statement": [
         *         {
         *             "Effect": "Allow",
         *             "Principal": {
         *                 "AWS": "arn:aws:iam::757278738533:root"
         *             },
         *             "Action": "sts:AssumeRole",
         *             "Condition": {
         *                 "StringEquals": {
         *                     "sts:ExternalId": "aW5zdGFuY2VfbWVpeWlfZGV2"
         *                 }
         *             }
         *         }
         *     ]
         * }
         */
        String policyDocument = "{\n" +
                    "    \"Version\": \"2012-10-17\",\n" +
                    "    \"Statement\": [\n" +
                    "        {\n" +
                    "            \"Effect\": \"Allow\",\n" +
                    "            \"Principal\": {\n" +
                    "                \"AWS\": \"arn:aws:iam::" + instanceRamUserId + ":root\"\n" +
                    "            },\n" +
                    "            \"Action\": \"sts:AssumeRole\",\n" +
                    "            \"Condition\": {\n" +
                    "                \"StringEquals\": {\n" +
                    "                    \"sts:ExternalId\": \"" + externalId + "\"\n" +
                    "                }\n" +
                    "            }\n" +
                    "        }\n" +
                    "    ]\n" +
                    "}";
        software.amazon.awssdk.services.iam.model.CreateRoleRequest request = software.amazon.awssdk.services.iam.model.CreateRoleRequest.builder()
                .roleName(roleName).assumeRolePolicyDocument(policyDocument).description(roleDescription).build()
        software.amazon.awssdk.services.iam.model.CreateRoleResponse response = iam.createRole(request)

        // attach policy
        attachPolicy(provider, region, ak, sk, roleName, policy)
        return new String[]{roleName, response.role().arn()}
    }
    throw new Exception("Unsupported provider [" + provider + "] for create role")
}
logger.info("Added 'createRole' function to Suite")

// cos and obs do not need to detach policy when delete role
Suite.metaClass.detachPolicy = { String provider, String region, String ak, String sk, String roleName, String policyName ->
    Suite suite = delegate as Suite
    suite.getLogger().info("cloud_stage_ram plugin: suiteName: ${suite.name}, detachPolicy, provider: ${provider}, region: ${region}, roleName: ${roleName}".toString())

    if (provider.equalsIgnoreCase("oss")) {
        DetachPolicyFromRoleRequest detachPolicyFromRoleRequest = new DetachPolicyFromRoleRequest()
        detachPolicyFromRoleRequest.setPolicyType("Custom");
        detachPolicyFromRoleRequest.setPolicyName(policyName);
        detachPolicyFromRoleRequest.setRoleName(roleName);
        try {
            DefaultProfile profile = DefaultProfile.getProfile(region, ak, sk)
            IAcsClient client = new DefaultAcsClient(profile)
            DetachPolicyFromRoleResponse response = client.getAcsResponse(detachPolicyFromRoleRequest)
        } catch (Exception e) {
            suite.getLogger().warn("Failed detach policy for role [${roleName}] and policy [${policyName}], " + e.getMessage())
        }
    } else if (provider.equalsIgnoreCase("s3")) {
        try {
            AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(ak, sk)
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(basicCredentials)
            software.amazon.awssdk.services.iam.IamClient iam = software.amazon.awssdk.services.iam.IamClient.builder()
                    .region(Region.of(region)).credentialsProvider(scp).build()
            DetachRolePolicyRequest request = DetachRolePolicyRequest.builder()
                    .roleName(roleName).policyArn(policyName).build()
            iam.detachRolePolicy(request)
        } catch (Exception e) {
            suite.getLogger().warn("Failed detach policy for role [${roleName}] and policy [${policyName}], " + e.getMessage())
        }
    } else {
        throw new Exception("Unsupported provider [" + provider + "] for attach policy")
    }
}
logger.info("Added 'detachPolicy' function to Suite")

// oss: detach policy firstly
// cos: does not need to detach policy when delete role
// obs:
// s3:  detach policy firstly; policyName is policy arn;
Suite.metaClass.deleteRole = { String provider, String region, String ak, String sk, String instanceId, String policyName ->
    Suite suite = delegate as Suite
    suite.getLogger().info("cloud_stage_ram plugin: suiteName: ${suite.name}, deleteRole, provider: ${provider}, region: ${region}, instanceId: ${instanceId}".toString())
    def roleName = getRoleName(instanceId)

    if (provider.equalsIgnoreCase("oss") || provider.equalsIgnoreCase("obs")) {
        return
    }

    if(provider.equalsIgnoreCase("cos")) {
        // cos does not need to detach policy when delete role
        com.tencentcloudapi.cam.v20190116.models.DeleteRoleRequest deleteRoleRequest = new com.tencentcloudapi.cam.v20190116.models.DeleteRoleRequest()
        deleteRoleRequest.setRoleName(roleName)
        try {
            Credential cred = new Credential(ak, sk)
            CamClient client = new CamClient(cred, "")
            com.tencentcloudapi.cam.v20190116.models.DeleteRoleResponse deleteRoleResponse = client.DeleteRole(deleteRoleRequest)
        } catch (Exception e) {
            suite.getLogger().warn("Failed delete role for role [${roleName}], " + e.getMessage())
        }
    } else if(provider.equalsIgnoreCase("s3")) {
        detachPolicy(provider, region, ak, sk, roleName, policyName)
        try {
            AwsBasicCredentials basicCredentials = AwsBasicCredentials.create(ak, sk)
            StaticCredentialsProvider scp = StaticCredentialsProvider.create(basicCredentials)
            software.amazon.awssdk.services.iam.IamClient iam = software.amazon.awssdk.services.iam.IamClient.builder()
                    .region(Region.of(region)).credentialsProvider(scp).build()
            software.amazon.awssdk.services.iam.model.DeleteRoleRequest request = software.amazon.awssdk.services.iam.model.DeleteRoleRequest.builder().roleName(roleName).build();
            software.amazon.awssdk.services.iam.model.DeleteRoleResponse response = iam.deleteRole(request);
        } catch (Exception e) {
            suite.getLogger().warn("Failed delete role for role [${roleName}], " + e.getMessage())
        }
    } else if (provider.equalsIgnoreCase("oss")) { // useful when oss support external id
        detachPolicy(provider, region, ak, sk, roleName, policyName)
        DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest()
        deleteRoleRequest.setRoleName(roleName)
        try {
            DefaultProfile profile = DefaultProfile.getProfile(region, ak, sk)
            IAcsClient client = new DefaultAcsClient(profile)
            DeleteRoleResponse response = client.getAcsResponse(deleteRoleRequest)
        } catch (Exception e) {
            suite.getLogger().warn("Failed delete role for role [${roleName}], " + e.getMessage())
        }
    } else {
        throw new Exception("Unsupported provider [" + provider + "] for delete role")
    }
}
logger.info("Added 'deleteRole' function to Suite")