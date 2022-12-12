package org.apache.doris.udf.demo;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.lang3.StringUtils;

public class AESDecrypt extends UDF {

    public String evaluate(String content, String secret) throws Exception {
        if (StringUtils.isBlank(content)) {
            throw new Exception("content not is null");
        }
        if (StringUtils.isBlank(secret)) {
            throw new Exception("Secret not is null");
        }
        return AESUtil.decrypt(content, secret);
    }

}
