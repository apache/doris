import org.apache.doris.sdk.DorisValueReader;

import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DorisValueReaderTest {

    @Test
    public void testClient() throws TException, IOException {
        int offset = 0;
        DorisValueReader dorisValueReader = new DorisValueReader(buildRequiredParams());
        while (dorisValueReader.hasNext()) {
            Object obj = dorisValueReader.getNext();
            System.out.println(obj);
            offset++;
        }
        System.out.println("offset: " + offset);
        dorisValueReader.close();
    }

    private Map<String, String> buildRequiredParams() {
        Map<String, String> requiredParams = new HashMap<>();
        requiredParams.put("fe_host", "127.0.0.1");
        requiredParams.put("fe_http_port", "8030");
        requiredParams.put("database", "test");
        requiredParams.put("table", "test_source");
        requiredParams.put("username", "root");
        requiredParams.put("password", "");
        requiredParams.put("sql", "select * from test.test_source");
        return requiredParams;
    }

}
