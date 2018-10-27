package com.baidu.palo.catalog;

import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.common.FeConstants;
import com.baidu.palo.common.io.FastByteArrayOutputStream;
import com.baidu.palo.common.util.UnitTestUtil;

import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import mockit.NonStrictExpectations;
import mockit.internal.startup.Startup;

public class OlapTableTest {

    static {
        Startup.initializeIfPossible();
    }

    @Test
    public void test() throws IOException {
        
        new NonStrictExpectations(Catalog.class) {
            {
                Catalog.getCurrentCatalogJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;
            }
        };

        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, 8);
        List<Table> tables = db.getTables();
        
        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable tbl = (OlapTable) table;
            System.out.println("orig table id: " + tbl.getId());

            FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
            tbl.write(out);

            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());
            Table copiedTbl = OlapTable.read(in);
            System.out.println("copied table id: " + copiedTbl.getId());
        }
        
    }

}
