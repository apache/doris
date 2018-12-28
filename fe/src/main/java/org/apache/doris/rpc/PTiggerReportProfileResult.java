package org.apache.doris.rpc;

import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class PTiggerReportProfileResult {
    @Protobuf(order = 1, required = true)
    public PStatus status;

    public PTiggerReportProfileResult() {
    }
}
