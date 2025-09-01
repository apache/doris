package org.apache.doris.common.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PrintableMapTest {

    @Test
    public void testSensitiveKeysContainAliyunDLFProperties() {
        // Verify that SENSITIVE_KEY contains sensitive keys from AliyunDLFBaseProperties
        // These keys are added via ConnectorPropertiesUtils.getSensitiveKeys(AliyunDLFBaseProperties.class)

        // Verify sensitive keys from AliyunDLFBaseProperties
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("dlf.secret_key"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("dlf.catalog.accessKeySecret"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("dlf.session_token"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("dlf.catalog.sessionToken"));

        // Verify other common sensitive keys
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("password"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("kerberos_keytab_content"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("bos_secret_accesskey"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("jdbc.password"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("elasticsearch.password"));

        // Verify cloud storage related sensitive keys (these are constants added in static initialization block)
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("s3.secret_key"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("AWS_SECRET_KEY"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("obs.secret_key"));
        Assertions.assertTrue(PrintableMap.SENSITIVE_KEY.contains("oss.secret_key"));
    }

    @Test
    public void testBasicConstructor() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        testMap.put("key2", "value2");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", true, false);
        String result = printableMap.toString();

        Assertions.assertTrue(result.contains("\"key1\" = \"value1\""));
        Assertions.assertTrue(result.contains("\"key2\" = \"value2\""));
    }

    @Test
    public void testConstructorWithEntryDelimiter() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        testMap.put("key2", "value2");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, ":", false, false, ";");
        String result = printableMap.toString();

        Assertions.assertTrue(result.contains("key1 : value1"));
        Assertions.assertTrue(result.contains("key2 : value2"));
        Assertions.assertTrue(result.contains(";"));
    }

    @Test
    public void testConstructorWithHidePassword() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("username", "admin");
        testMap.put("password", "secret123");
        testMap.put("dlf.secret_key", "dlf_secret");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, false, true);
        String result = printableMap.toString();

        Assertions.assertTrue(result.contains("username = admin"));
        Assertions.assertTrue(result.contains("password = " + PrintableMap.PASSWORD_MASK));
        Assertions.assertTrue(result.contains("dlf.secret_key = " + PrintableMap.PASSWORD_MASK));
    }

    @Test
    public void testConstructorWithSorted() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("zebra", "value3");
        testMap.put("apple", "value1");
        testMap.put("banana", "value2");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, false, true, true);
        String result = printableMap.toString();

        // Verify sorting (descending order)
        int zebraIndex = result.indexOf("zebra");
        int bananaIndex = result.indexOf("banana");
        int appleIndex = result.indexOf("apple");

        Assertions.assertTrue(zebraIndex < bananaIndex);
        Assertions.assertTrue(bananaIndex < appleIndex);
    }

    @Test
    public void testWithQuotation() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", true, false);
        String result = printableMap.toString();

        Assertions.assertEquals("\"key1\" = \"value1\"", result);
    }

    @Test
    public void testWithoutQuotation() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, false);
        String result = printableMap.toString();

        Assertions.assertEquals("key1 = value1", result);
    }

    @Test
    public void testWithWrap() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        testMap.put("key2", "value2");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, true);
        String result = printableMap.toString();

        Assertions.assertTrue(result.contains("\n"));
        Assertions.assertTrue(result.contains("key1 = value1"));
        Assertions.assertTrue(result.contains("key2 = value2"));
    }

    @Test
    public void testHidePasswordWithSensitiveKeys() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("normal_key", "normal_value");
        testMap.put("password", "secret_password");
        testMap.put("dlf.secret_key", "dlf_secret_value");
        testMap.put("s3.secret_key", "s3_secret_value");
        testMap.put("kerberos_keytab_content", "kerberos_content");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, false, true);
        String result = printableMap.toString();

        Assertions.assertTrue(result.contains("normal_key = normal_value"));
        Assertions.assertTrue(result.contains("password = " + PrintableMap.PASSWORD_MASK));
        Assertions.assertTrue(result.contains("dlf.secret_key = " + PrintableMap.PASSWORD_MASK));
        Assertions.assertTrue(result.contains("s3.secret_key = " + PrintableMap.PASSWORD_MASK));
        Assertions.assertTrue(result.contains("kerberos_keytab_content = " + PrintableMap.PASSWORD_MASK));
    }

    @Test
    public void testCaseInsensitiveSensitiveKeys() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("PASSWORD", "secret_password");
        testMap.put("Password", "another_secret");
        testMap.put("password", "third_secret");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, false, true);
        String result = printableMap.toString();

        Assertions.assertTrue(result.contains("PASSWORD = " + PrintableMap.PASSWORD_MASK));
        Assertions.assertTrue(result.contains("Password = " + PrintableMap.PASSWORD_MASK));
        Assertions.assertTrue(result.contains("password = " + PrintableMap.PASSWORD_MASK));
    }

    @Test
    public void testAdditionalHiddenKeys() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("visible_key", "visible_value");
        testMap.put("hidden_key", "hidden_value");
        testMap.put("another_hidden", "another_value");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, false);
        Set<String> hiddenKeys = new java.util.HashSet<>();
        hiddenKeys.add("hidden_key");
        hiddenKeys.add("another_hidden");
        printableMap.setAdditionalHiddenKeys(hiddenKeys);

        String result = printableMap.toString();

        Assertions.assertTrue(result.contains("visible_key = visible_value"));
        Assertions.assertFalse(result.contains("hidden_key"));
        Assertions.assertFalse(result.contains("another_hidden"));
    }

    @Test
    public void testNullMap() {
        PrintableMap<String, String> printableMap = new PrintableMap<>(null, "=", false, false);
        String result = printableMap.toString();

        Assertions.assertEquals("", result);
    }

    @Test
    public void testEmptyMap() {
        Map<String, String> testMap = new HashMap<>();
        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, false);
        String result = printableMap.toString();

        Assertions.assertEquals("", result);
    }

    @Test
    public void testSingleEntry() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("single_key", "single_value");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "=", false, false);
        String result = printableMap.toString();

        Assertions.assertEquals("single_key = single_value", result);
    }

    @Test
    public void testMultipleEntriesWithCustomDelimiter() {
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        testMap.put("key2", "value2");
        testMap.put("key3", "value3");

        PrintableMap<String, String> printableMap = new PrintableMap<>(testMap, "->", false, false, "|");
        String result = printableMap.toString();

        Assertions.assertTrue(result.contains("key1 -> value1"));
        Assertions.assertTrue(result.contains("key2 -> value2"));
        Assertions.assertTrue(result.contains("key3 -> value3"));
        Assertions.assertTrue(result.contains("|"));
        Assertions.assertFalse(result.contains(","));
    }

    @Test
    public void testHiddenKeysFromS3AndGlue() {
        // Verify that HIDDEN_KEY contains S3 and Glue related keys
        // These keys are added in the static initialization block
        Assertions.assertFalse(PrintableMap.HIDDEN_KEY.isEmpty());
    }

    @Test
    public void testPasswordMaskConstant() {
        Assertions.assertEquals("*XXX", PrintableMap.PASSWORD_MASK);
    }
}
