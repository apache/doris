package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.ConnectorProperty;

import java.util.Map;

public abstract class AWSGlueMetaStoreBaseProperties extends MetastoreProperties{
    @ConnectorProperty(names = {"glue.endpoint", "aws.endpoint", "aws.glue.endpoint"},
            description = "The endpoint of the AWS Glue.")
    protected String glueEndpoint = "";
    
    @ConnectorProperty(names = {"glue.region", "aws.region", "aws.glue.region"},
            description = "The region of the AWS Glue. " +
                    "If not set, it will use the default region configured in the AWS SDK or environment variables."
    )
    protected String glueRegion = "";

    /** AWS credentials. */
    @ConnectorProperty(names = {"client.credentials-provider"},
            description = "The class name of the credentials provider for AWS Glue.",
            supported = false)
    protected String credentialsProviderClass = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";

    @ConnectorProperty(names = {"glue.access_key",
            "aws.glue.access-key", "client.credentials-provider.glue.access_key"},
            description = "The access key of the AWS Glue.")
    protected String glueAccessKey = "";

    @ConnectorProperty(names = {"glue.secret_key",
            "aws.glue.secret-key", "client.credentials-provider.glue.secret_key"},
            description = "The secret key of the AWS Glue.")
    protected String glueSecretKey = "";

    @ConnectorProperty(names = {"glue.role_arn"},
            description = "The IAM role the AWS Glue.",
            supported = false)
    protected String glueIAMRole = "";

    @ConnectorProperty(names = {"glue.external_id"},
            description = "The external id of the AWS Glue.",
            supported = false)
    protected String glueExternalId = "";
    
    /**
     * Base constructor for subclasses to initialize the common state.
     *
     * @param type      metastore type
     * @param origProps original configuration
     */
    protected AWSGlueMetaStoreBaseProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }
}
