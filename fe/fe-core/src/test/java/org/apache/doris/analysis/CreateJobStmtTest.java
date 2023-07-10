package org.apache.doris.analysis;

import org.apache.doris.common.util.SqlParserUtils;
import org.junit.jupiter.api.Test;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;

import java.io.StringReader;

public class CreateJobStmtTest {
    
    
    @Test
    public void pauseAllJob(){
      SqlParser parser = new SqlParser(new SqlScanner(new StringReader("pause "), 1L));
        SqlParserUtils.getFirstStmt(parser);
    }
}
