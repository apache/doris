package org.apache.doris.cdcclient.model.resp;

import java.util.Map;
import lombok.Data;

@Data
public class WriteMetaResp {
    private Map<String, String> meta;
}
