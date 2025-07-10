package org.apache.doris.datasource.property.metastore;

import org.apache.doris.common.UserException;

import java.util.Map;

public class IcebergPropertiesFactory implements MetastorePropertiesFactory{
    @Override
    public MetastoreProperties create(Map<String, String> props) throws UserException {
        return null;
    }
}
