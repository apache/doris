package org.apache.doris.datasource.s3tables;

import org.apache.doris.datasource.iceberg.s3tables.CustomAwsS3TablesCredentialsProvider;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3TablesTest {

    @Test
    public void testS3TablesCatalog() {
        S3TablesCatalog s3TablesCatalog = new S3TablesCatalog();
        Map<String, String> s3Properties = new HashMap<>();

        // ak, sk
        String accessKeyId = "";
        String secretKey = "";

        s3Properties.put("client.region", "us-east-1");
        s3Properties.put("client.credentials-provider", CustomAwsS3TablesCredentialsProvider.class.getName());
        s3Properties.put("client.credentials-provider.s3.access-key-id", accessKeyId);
        s3Properties.put("client.credentials-provider.s3.secret-access-key", secretKey);

        String warehouse = "arn:aws:s3tables:us-east-1:169698404049:bucket/yy-s3-table-bucket";
        s3Properties.put("warehouse", warehouse);

        try {
            s3TablesCatalog.initialize("s3tables", s3Properties);
            System.out.println("Successfully initialized S3 Tables catalog!");

            try {
                // 1. list namespaces
                List<Namespace> namespaces = s3TablesCatalog.listNamespaces();
                System.out.println("Successfully listed namespaces:");
                for (Namespace namespace : namespaces) {
                    System.out.println(namespace);
                    // 2. list tables
                    List<TableIdentifier> tblIdentifiers = s3TablesCatalog.listTables(namespace);
                    for (TableIdentifier tblId : tblIdentifiers) {
                        // 3. load table and list files
                        System.out.println(tblId);
                        Table tbl = s3TablesCatalog.loadTable(tblId);
                        System.out.println(tbl.schema());
                        TableScan scan = tbl.newScan();
                        CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();
                        for (FileScanTask task : fileScanTasks) {
                            System.out.println(task.file());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Note: Could not list namespaces - " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Error connecting to S3 Tables: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
