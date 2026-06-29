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

package org.apache.doris.common.util;

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.credentials.CloudCredential;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Util {
    private static final Logger LOG = LogManager.getLogger(Util.class);

    private static AwsCredentialsProvider getAwsCredencialsProvider(CloudCredential credential) {
        AwsCredentials awsCredential;
        AwsCredentialsProvider awsCredentialsProvider;
        if (!credential.isTemporary()) {
            awsCredential = AwsBasicCredentials.create(credential.getAccessKey(), credential.getSecretKey());
        } else {
            awsCredential = AwsSessionCredentials.create(credential.getAccessKey(), credential.getSecretKey(),
                        credential.getSessionToken());
        }

        if (!credential.isWhole()) {
            awsCredentialsProvider = AwsCredentialsProviderChain.of(
                    SystemPropertyCredentialsProvider.create(),
                    EnvironmentVariableCredentialsProvider.create(),
                    WebIdentityTokenFileCredentialsProvider.create(),
                    ProfileCredentialsProvider.create(),
                    InstanceProfileCredentialsProvider.create());
        } else {
            awsCredentialsProvider = StaticCredentialsProvider.create(awsCredential);
        }

        return awsCredentialsProvider;
    }

    @Deprecated
    public static S3Client buildS3Client(URI endpoint, String region, CloudCredential credential,
            boolean isUsePathStyle) {
        EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy
                .builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
        // retry 3 time with Equal backoff
        RetryPolicy retryPolicy = RetryPolicy
                .builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration
                .builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30)).build())
                .endpointOverride(endpoint)
                .credentialsProvider(getAwsCredencialsProvider(credential))
                .region(Region.of(region))
                .overrideConfiguration(clientConf)
                // disable chunkedEncoding because of bos not supported
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(isUsePathStyle)
                        .build())
                .build();
    }

    /**
     * Using (accessKey, secretKey) or roleArn for creating different credentials provider when creating s3client
     * @param endpoint AWS endpoint (eg: "https://s3.us-east-1.amazonaws.com")
     * @param region AWS region identifier (eg: "us-east-1")
     * @param accessKey AWS access key ID
     * @param secretKey AWS secret access key, paired with accessKey
     * @param sessionToken AWS temporary session token for short-term credentials
     * @param roleArn AWS iam role arn to assume (format: "arn:aws:iam::123456789012:role/role-name")
     * @param externalId  AWS External ID for cross-account role assumption security
     * @return
     */
    private static AwsCredentialsProvider getAwsCredencialsProviderV1(URI endpoint, String region, String accessKey,
            String secretKey, String sessionToken, String roleArn, String externalId) {

        if (!Strings.isNullOrEmpty(accessKey) && !Strings.isNullOrEmpty(secretKey)) {
            if (Strings.isNullOrEmpty(sessionToken)) {
                return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
            } else {
                return StaticCredentialsProvider.create(AwsSessionCredentials.create(accessKey,
                        secretKey, sessionToken));
            }
        }

        if (!Strings.isNullOrEmpty(roleArn)) {
            StsClient stsClient = StsClient.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();

            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(builder -> {
                        builder.roleArn(roleArn).roleSessionName("aws-sdk-java-v2-fe");
                        if (!Strings.isNullOrEmpty(externalId)) {
                            builder.externalId(externalId);
                        }
                    }).build();
        }
        return DefaultCredentialsProvider.create();
    }

    private static AwsCredentialsProvider getAwsCredencialsProviderV2(URI endpoint, String region, String accessKey,
            String secretKey, String sessionToken, String roleArn, String externalId) {

        if (!Strings.isNullOrEmpty(accessKey) && !Strings.isNullOrEmpty(secretKey)) {
            if (Strings.isNullOrEmpty(sessionToken)) {
                return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
            } else {
                return StaticCredentialsProvider.create(AwsSessionCredentials.create(accessKey,
                        secretKey, sessionToken));
            }
        }

        if (!Strings.isNullOrEmpty(roleArn)) {
            StsClient stsClient = StsClient.builder()
                    .credentialsProvider(AwsCredentialsProviderChain.of(
                            WebIdentityTokenFileCredentialsProvider.create(),
                            ContainerCredentialsProvider.create(),
                            InstanceProfileCredentialsProvider.create(),
                            SystemPropertyCredentialsProvider.create(),
                            EnvironmentVariableCredentialsProvider.create(),
                            ProfileCredentialsProvider.create()))
                    .build();

            return StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(builder -> {
                        builder.roleArn(roleArn).roleSessionName("aws-sdk-java-v2-fe");
                        if (!Strings.isNullOrEmpty(externalId)) {
                            builder.externalId(externalId);
                        }
                    }).build();
        }
        return AwsCredentialsProviderChain.of(
                            WebIdentityTokenFileCredentialsProvider.create(),
                            ContainerCredentialsProvider.create(),
                            InstanceProfileCredentialsProvider.create(),
                            SystemPropertyCredentialsProvider.create(),
                            EnvironmentVariableCredentialsProvider.create(),
                            ProfileCredentialsProvider.create());
    }

    private static AwsCredentialsProvider getAwsCredencialsProvider(URI endpoint, String region, String accessKey,
                String secretKey, String sessionToken, String roleArn, String externalId) {
        if (Config.aws_credentials_provider_version.equalsIgnoreCase("v2")) {
            return getAwsCredencialsProviderV2(endpoint, region, accessKey, secretKey,
                    sessionToken, roleArn, externalId);
        }
        return getAwsCredencialsProviderV1(endpoint, region, accessKey, secretKey,
                sessionToken, roleArn, externalId);
    }

    public static S3Client buildS3Client(URI endpoint, String region, boolean isUsePathStyle,
                                         AwsCredentialsProvider credential) {
        EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy
                .builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
        // retry 3 time with Equal backoff
        RetryPolicy retryPolicy = RetryPolicy
                .builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration
                .builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30)).build())
                .endpointOverride(endpoint)
                .credentialsProvider(credential)
                .region(Region.of(region))
                .overrideConfiguration(clientConf)
                // disable chunkedEncoding because of bos not supported
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(isUsePathStyle)
                        .build())
                .build();
    }

    public static S3Client buildS3Client(URI endpoint, String region, boolean isUsePathStyle, String accessKey,
            String secretKey, String sessionToken, String roleArn, String externalId) {
        EqualJitterBackoffStrategy backoffStrategy = EqualJitterBackoffStrategy
                .builder()
                .baseDelay(Duration.ofSeconds(1))
                .maxBackoffTime(Duration.ofMinutes(1))
                .build();
        // retry 3 time with Equal backoff
        RetryPolicy retryPolicy = RetryPolicy
                .builder()
                .numRetries(3)
                .backoffStrategy(backoffStrategy)
                .build();
        ClientOverrideConfiguration clientConf = ClientOverrideConfiguration
                .builder()
                // set retry policy
                .retryPolicy(retryPolicy)
                // using AwsS3V4Signer
                .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
                .build();
        return S3Client.builder()
                .httpClient(UrlConnectionHttpClient.builder().socketTimeout(Duration.ofSeconds(30))
                        .connectionTimeout(Duration.ofSeconds(30)).build())
                .endpointOverride(endpoint)
                .credentialsProvider(getAwsCredencialsProvider(endpoint, region, accessKey, secretKey,
                        sessionToken, roleArn, externalId))
                .region(Region.of(region))
                .overrideConfiguration(clientConf)
                // disable chunkedEncoding because of bos not supported
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(isUsePathStyle)
                        .build())
                .build();
    }

    public static String getLongestPrefix(String globPattern) {
        int length = globPattern.length();
        int earliestSpecialCharIndex = length;

        char[] specialChars = {'*', '?', '[', '{', '\\'};

        for (char specialChar : specialChars) {
            int index = globPattern.indexOf(specialChar);
            if (index != -1 && index < earliestSpecialCharIndex) {
                earliestSpecialCharIndex = index;
            }
        }

        return globPattern.substring(0, earliestSpecialCharIndex);
    }

    // Apply some rules to extend the globs parsing behavior
    public static String extendGlobs(String pathPattern) {
        return extendGlobNumberRange(pathPattern);
    }

    /**
     * Convert range patterns to brace enumeration patterns for glob matching.
     * Parts containing negative numbers or non-numeric characters are skipped.
     * eg(valid):
     *    -> "file{1..3}" => "file{1,2,3}"
     *    -> "file_{1..3,4,5..6}" => "file_{1,2,3,4,5,6}"
     * eg(invalid)
     *    -> "data_{-1..4}.csv" will not load any file
     *    -> "data_{a..4}.csv" will not load any file
     * @param pathPattern Path that may contain {start..end} or mixed {start..end,values} patterns
     * @return Path with ranges converted to comma-separated enumeration
     */
    public static String extendGlobNumberRange(String pathPattern) {
        Pattern bracePattern = Pattern.compile("\\{([^}]+)\\}");
        Matcher braceMatcher = bracePattern.matcher(pathPattern);
        StringBuffer result = new StringBuffer();

        while (braceMatcher.find()) {
            String braceContent = braceMatcher.group(1);
            String[] parts = braceContent.split(",");
            List<Integer> allNumbers = new ArrayList<>();
            Pattern rangePattern = Pattern.compile("^(-?\\d+)\\.\\.(-?\\d+)$");

            for (String part : parts) {
                part = part.trim();
                Matcher rangeMatcher = rangePattern.matcher(part);

                if (rangeMatcher.matches()) {
                    int start = Integer.parseInt(rangeMatcher.group(1));
                    int end = Integer.parseInt(rangeMatcher.group(2));

                    // Skip this range if either start or end is negative
                    if (start < 0 || end < 0) {
                        continue;
                    }

                    if (start > end) {
                        int temp = start;
                        start = end;
                        end = temp;
                    }
                    for (int i = start; i <= end; i++) {
                        if (!allNumbers.contains(i)) {
                            allNumbers.add(i);
                        }
                    }
                } else if (part.matches("^\\d+$")) {
                    // This is a single non-negative number like "4"
                    int num = Integer.parseInt(part);
                    if (!allNumbers.contains(num)) {
                        allNumbers.add(num);
                    }
                } else {
                    // Not a valid number or range (e.g., negative number, or contains non-numeric chars)
                    // Just skip this part and continue processing other parts
                    continue;
                }
            }

            // If no valid numbers found after filtering, keep original content
            if (allNumbers.isEmpty()) {
                braceMatcher.appendReplacement(result, "{" + braceContent + "}");
                continue;
            }

            // Build comma-separated result
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < allNumbers.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append(allNumbers.get(i));
            }
            sb.append("}");
            braceMatcher.appendReplacement(result, sb.toString());
        }
        braceMatcher.appendTail(result);

        return result.toString();
    }

    // Fast fail validation for S3 endpoint connectivity to avoid retries and long waits
    // when network conditions are poor. Validates endpoint format, whitelist, security,
    // and tests connection with 10s timeout.
    public static void validateAndTestEndpoint(String endpoint) throws UserException {
        HttpURLConnection connection = null;
        try {
            String urlStr = endpoint;
            // Add default protocol if not specified
            if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
                urlStr = "http://" + endpoint;
            }
            endpoint = endpoint.replaceFirst("^http://", "");
            endpoint = endpoint.replaceFirst("^https://", "");
            List<String> whiteList = new ArrayList<>(Arrays.asList(Config.s3_load_endpoint_white_list));
            whiteList.removeIf(String::isEmpty);
            if (!whiteList.isEmpty() && !whiteList.contains(endpoint)) {
                throw new UserException("endpoint: " + endpoint
                    + " is not in s3 load endpoint white list: " + String.join(",", whiteList));
            }
            SecurityChecker.getInstance().startSSRFChecking(urlStr);
            URL url = new URL(urlStr);
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(10000);
            connection.connect();
        } catch (Exception e) {
            String msg;
            if (e instanceof UserException) {
                msg = ((UserException) e).getDetailMessage();
            } else {
                LOG.warn("Failed to connect endpoint={}, err={}", endpoint, e);
                msg = e.getMessage();
            }
            throw new UserException(InternalErrorCode.GET_REMOTE_DATA_ERROR,
                "Failed to access object storage, message=" + msg, e);
        } finally {
            if (connection != null) {
                try {
                    connection.disconnect();
                } catch (Exception e) {
                    LOG.warn("Failed to disconnect connection, endpoint={}, err={}", endpoint, e);
                }
            }
            SecurityChecker.getInstance().stopSSRFChecking();
        }
    }

    /**
     * Check if a path pattern is deterministic, meaning all file paths can be determined
     * without listing. A pattern is deterministic if it contains no true wildcard characters
     * (*, ?) but may contain brace patterns ({...}) and non-negated bracket patterns ([abc], [0-9])
     * which can be expanded to concrete paths.
     *
     * Negated bracket patterns ([!abc], [^abc]) are NOT deterministic because they match
     * any character except those listed, requiring a listing to discover matches.
     *
     * This allows skipping S3 ListBucket operations when only GetObject permission is available.
     *
     * @param pathPattern Path that may contain glob patterns
     * @return true if the pattern is deterministic (expandable without listing)
     */
    public static boolean isDeterministicPattern(String pathPattern) {
        // Check for wildcard characters that require listing
        // Note: '{' is NOT a wildcard - it's a brace expansion pattern that can be deterministically expanded
        // Note: '[' is conditionally deterministic - [abc] can be expanded, but [!abc]/[^abc] cannot
        char[] wildcardChars = {'*', '?'};
        for (char c : wildcardChars) {
            if (pathPattern.indexOf(c) != -1) {
                return false;
            }
        }
        // Check for escaped characters which indicate complex patterns
        if (pathPattern.indexOf('\\') != -1) {
            return false;
        }
        // Check bracket patterns: [abc] and [0-9] are deterministic, [!abc] and [^abc] are not
        if (!areBracketPatternsDeterministic(pathPattern)) {
            return false;
        }
        return true;
    }

    /**
     * Check if all bracket patterns in the path are deterministic (non-negated).
     * - [abc], [0-9], [a-zA-Z] are deterministic (can be expanded to finite character sets)
     * - [!abc], [^abc] are non-deterministic (negation requires listing)
     * - Malformed brackets (no closing ]) are non-deterministic
     */
    private static boolean areBracketPatternsDeterministic(String pattern) {
        int i = 0;
        while (i < pattern.length()) {
            if (pattern.charAt(i) == '[') {
                int end = pattern.indexOf(']', i + 1);
                if (end == -1) {
                    // Malformed bracket - no closing ], treat as non-deterministic
                    return false;
                }
                int contentStart = i + 1;
                if (contentStart == end) {
                    // Empty brackets [] - malformed, treat as non-deterministic
                    return false;
                }
                // Check for negation
                char first = pattern.charAt(contentStart);
                if (first == '!' || first == '^') {
                    return false;
                }
                i = end + 1;
            } else {
                i++;
            }
        }
        return true;
    }

    /**
     * Expand bracket character class patterns to brace patterns.
     * This converts [abc] to {a,b,c} and [0-9] to {0,1,2,...,9} so that
     * the existing brace expansion can handle them.
     *
     * Only call this on patterns already verified as deterministic by isDeterministicPattern()
     * (i.e., no negated brackets like [!...] or [^...]).
     *
     * Examples:
     *   - "file[abc].csv" => "file{a,b,c}.csv"
     *   - "file[0-9].csv" => "file{0,1,2,3,4,5,6,7,8,9}.csv"
     *   - "file[a-cX].csv" => "file{a,b,c,X}.csv"
     *   - "file.csv" => "file.csv" (no brackets)
     *
     * @param pathPattern Path with optional bracket patterns (must not contain negated brackets)
     * @return Path with brackets converted to brace patterns
     */
    public static String expandBracketPatterns(String pathPattern) {
        StringBuilder result = new StringBuilder();
        int i = 0;
        while (i < pathPattern.length()) {
            if (pathPattern.charAt(i) == '[') {
                int end = pathPattern.indexOf(']', i + 1);
                if (end == -1) {
                    // Malformed, keep as-is
                    result.append(pathPattern.charAt(i));
                    i++;
                    continue;
                }
                String content = pathPattern.substring(i + 1, end);
                List<Character> chars = expandBracketContent(content);
                result.append('{');
                for (int j = 0; j < chars.size(); j++) {
                    if (j > 0) {
                        result.append(',');
                    }
                    result.append(chars.get(j));
                }
                result.append('}');
                i = end + 1;
            } else {
                result.append(pathPattern.charAt(i));
                i++;
            }
        }
        return result.toString();
    }

    private static List<Character> expandBracketContent(String content) {
        List<Character> chars = new ArrayList<>();
        int i = 0;
        while (i < content.length()) {
            if (i + 2 < content.length() && content.charAt(i + 1) == '-') {
                // Range like a-z or 0-9
                char start = content.charAt(i);
                char end = content.charAt(i + 2);
                if (start <= end) {
                    for (char c = start; c <= end; c++) {
                        if (!chars.contains(c)) {
                            chars.add(c);
                        }
                    }
                } else {
                    for (char c = start; c >= end; c--) {
                        if (!chars.contains(c)) {
                            chars.add(c);
                        }
                    }
                }
                i += 3;
            } else {
                char c = content.charAt(i);
                if (!chars.contains(c)) {
                    chars.add(c);
                }
                i++;
            }
        }
        return chars;
    }

    /**
     * Expand brace patterns in a path to generate all concrete file paths.
     * Handles nested and multiple brace patterns.
     *
     * Examples:
     *   - "file{1,2,3}.csv" => ["file1.csv", "file2.csv", "file3.csv"]
     *   - "data/part{1..3}/file.csv" => ["data/part1/file.csv", "data/part2/file.csv", "data/part3/file.csv"]
     *   - "file.csv" => ["file.csv"] (no braces)
     *
     * @param pathPattern Path with optional brace patterns (already processed by extendGlobs)
     * @return List of expanded concrete paths
     */
    public static List<String> expandBracePatterns(String pathPattern) {
        List<String> result = new ArrayList<>();
        expandBracePatternsRecursive(pathPattern, result);
        return result;
    }

    private static void expandBracePatternsRecursive(String pattern, List<String> result) {
        int braceStart = pattern.indexOf('{');
        if (braceStart == -1) {
            // No more braces, add the pattern as-is
            result.add(pattern);
            return;
        }

        // Find matching closing brace (handle nested braces)
        int braceEnd = findMatchingBrace(pattern, braceStart);
        if (braceEnd == -1) {
            // Malformed pattern, treat as literal
            result.add(pattern);
            return;
        }

        String prefix = pattern.substring(0, braceStart);
        String braceContent = pattern.substring(braceStart + 1, braceEnd);
        String suffix = pattern.substring(braceEnd + 1);

        // Split by comma, but respect nested braces
        List<String> alternatives = splitBraceContent(braceContent);

        for (String alt : alternatives) {
            // Recursively expand any remaining braces in the suffix
            expandBracePatternsRecursive(prefix + alt + suffix, result);
        }
    }

    private static int findMatchingBrace(String pattern, int start) {
        int depth = 0;
        for (int i = start; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static List<String> splitBraceContent(String content) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(content.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(content.substring(start));
        return parts;
    }
}
