package org.apache.doris.udf.demo;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.lang3.StringUtils;

public class AESEncrypt extends UDF {

    public String evaluate(String content, String secret) throws Exception {
        if (StringUtils.isBlank(content)) {
            throw new Exception("content not is null");
        }
        if (StringUtils.isBlank(secret)) {
            throw new Exception("Secret not is null");
        }
        return AESUtil.encrypt(content, secret);
    }
}
