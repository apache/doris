package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.catalog.Namespace;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class IcebergMetadataOpTest {

    @Test
    public void testGetNamespaces() {
        Namespace ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1");
        Assert.assertEquals(1, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1.db2.db3");
        Assert.assertEquals(3, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "db1..db2");
        Assert.assertEquals(2, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.of("p1"), "db1");
        Assert.assertEquals(2, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.of("p1"), "");
        Assert.assertEquals(1, ns.length());

        ns = IcebergMetadataOps.getNamespace(Optional.empty(), "");
        Assert.assertEquals(0, ns.length());
    }
}
