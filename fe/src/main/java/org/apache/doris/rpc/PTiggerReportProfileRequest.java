package org.apache.doris.rpc;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.google.common.collect.Lists;

import java.util.List;

@ProtobufClass
public class PTiggerReportProfileRequest extends AttachmentRequest {

    @Protobuf(fieldType = FieldType.OBJECT, order = 1, required = false)
    List<PUniqueId> instanceIds;

    public PTiggerReportProfileRequest() {
    }

    public PTiggerReportProfileRequest(List<PUniqueId> instanceIds) {
        this.instanceIds = Lists.newArrayList();
        this.instanceIds.addAll(instanceIds);
    }
}
