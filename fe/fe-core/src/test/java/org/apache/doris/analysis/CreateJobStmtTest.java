package org.apache.doris.analysis;

import org.apache.doris.common.util.SqlParserUtils;
import org.junit.jupiter.api.Test;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;

import java.io.StringReader;

public class CreateJobStmtTest {
    
    
    @Test
    public void pauseAllJob() throws Exception {
        String sql = "CREATE EVENT job1 ON SCHEDULER EVERY  INTERVAL 1 DAY starts \"2023-02-15\" ends \"2023-02-15\" do \" select * from kris\" ;";
        String asSql = "CREATE EVENT job1 ON SCHEDULER AT \"2023-02-15\" do \"cccscsscsccs\";";
        
     // SqlParser parser = new SqlParser(new SqlScanner(new StringReader("stop routine load for ssss")));
        SqlScanner input = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(input);
       System.out.println(SqlParserUtils.getStmt(parser, 0).getClass().getSimpleName());
       // System.out.println(st.toSql());
    }
    
    public void createJobStmt(LabelName labelName, String onceJobStartTimestamp, Integer interval, String intervalTimeUnit,String startsTimeStamp, String endsTimeStamp, String doStmt) {
        
    }
        
    }
}
