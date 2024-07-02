package org.apache.doris.cdcloader.common.factory;

import org.apache.doris.cdcloader.mysql.reader.MySqlSourceReader;
import org.apache.doris.cdcloader.mysql.reader.SourceReader;

public class SourceReaderFactory {

    public static SourceReader createSourceReader(DataSource source) {
        switch (source){
            case MYSQL:
                return new MySqlSourceReader();
            default:
                throw new IllegalArgumentException("Unsupported SourceReader with datasource : " + source);
        }
    }
}
