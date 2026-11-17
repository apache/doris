// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestListFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.encryption.BaseEncryptedKey;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.kms.model.KmsException;

/** Test-only reference reader and fixture generator. Never install this class in a Doris plugin. */
public final class IcebergEncryptionFixture {
    private static final String DB = "iceberg_encryption_metadata_db";
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private static final String WAREHOUSE = "s3://iceberg-encryption/warehouse";
    private final String hms;
    private final String kms;
    private final String s3;
    private final String keyId;

    private IcebergEncryptionFixture(String[] args) throws IOException {
        hms = args[0];
        kms = args[1];
        s3 = args[2];
        Properties state = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(args[3]))) {
            state.load(input);
        }
        keyId = state.getProperty("key-id");
        check("1.12.0".equals(IcebergBuild.version()), "Fixture must use Iceberg 1.12.0");
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("Expected HMS URI, KMS endpoint, S3 endpoint and test key state");
        }
        IcebergEncryptionFixture fixture = new IcebergEncryptionFixture(args);
        HttpServer server = HttpServer.create(new InetSocketAddress(8181), 0);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        server.setExecutor(executor);
        server.createContext("/health", request -> reply(request, 200, "{\"ready\":true}"));
        server.createContext("/reset", request -> {
            try {
                check("POST".equals(request.getRequestMethod()), "POST required");
                fixture.reset();
                reply(request, 200, "{\"created\":true}");
            } catch (Exception | LinkageError e) {
                e.printStackTrace(System.err);
                reply(request, 500, "{\"error\":\"Fixture creation failed\"}");
            }
        });
        server.createContext("/verify", request -> {
            try {
                reply(request, 200, fixture.verify());
            } catch (Exception | LinkageError e) {
                e.printStackTrace(System.err);
                reply(request, 500, "{\"error\":\"Fixture verification failed\"}");
            }
        });
        // These AWS protocol errors isolate credentials/authorization from storage access and KMS availability.
        server.createContext("/kms/credentials", request -> reply(request, 400,
                "{\"__type\":\"UnrecognizedClientException\",\"message\":\"Invalid fixture credentials\"}"));
        server.createContext("/kms/denied", request -> reply(request, 403,
                "{\"__type\":\"AccessDeniedException\",\"message\":\"Fixture principal has no decrypt permission\"}"));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0);
            executor.shutdownNow();
        }));
        server.start();
        System.out.println("Iceberg 1.12.0 encrypted metadata fixture ready");
    }

    private Map<String, String> properties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.URI, hms);
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE);
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(CatalogProperties.ENCRYPTION_KMS_TYPE, "aws");
        properties.put("kms.endpoint", kms);
        properties.put("s3.endpoint", s3);
        properties.put("s3.path-style-access", "true");
        properties.put("client.region", "us-east-1");
        properties.put("http-client.type", "apache");
        properties.put("http-client.apache.connection-timeout-ms", "5000");
        properties.put("http-client.apache.socket-timeout-ms", "15000");
        properties.put("client.credentials-provider", FixtureCredentials.class.getName());
        properties.put("io.manifest.cache-enabled", "false");
        return properties;
    }

    private HiveCatalog catalog(Map<String, String> properties) {
        HiveCatalog catalog = new HiveCatalog();
        Configuration conf = new Configuration();
        conf.set("hive.metastore.client.socket.timeout", "15s");
        catalog.setConf(conf);
        catalog.initialize("encryption_fixture", properties);
        return catalog;
    }

    private synchronized void reset() throws Exception {
        try (HiveCatalog catalog = catalog(properties()); S3FileIO cleanup = new S3FileIO()) {
            cleanup.initialize(properties());
            Namespace namespace = Namespace.of(DB);
            if (!catalog.namespaceExists(namespace)) {
                catalog.createNamespace(namespace, Map.of("location", "file:/tmp/" + DB));
            }
            for (String name : List.of("encrypted_v3", "plain_v3")) {
                TableIdentifier id = TableIdentifier.of(DB, name);
                // HMS survives stop/up while Moto loses its keys. Purging via old encrypted
                // manifests would fail after dropping the HMS entry, leaking the old objects.
                // Only these dedicated fixture locations may be cleaned without reading manifests.
                catalog.dropTable(id, false);
                String location = WAREHOUSE + "/" + name;
                cleanup.deletePrefix(location + "/");
                check(!cleanup.listPrefix(location + "/").iterator().hasNext(), "Fixture cleanup is incomplete");
                Map<String, String> tableProperties = new HashMap<>();
                tableProperties.put(TableProperties.FORMAT_VERSION, "3");
                if (name.equals("encrypted_v3")) {
                    tableProperties.put(TableProperties.ENCRYPTION_TABLE_KEY, keyId);
                }
                Table table = catalog.buildTable(id, SCHEMA).withLocation(location)
                        .withProperties(tableProperties).create();
                append(table, 1, 2);
                append(table, 3, 4);
            }
        }
    }

    private static void append(Table table, int first, int last) throws Exception {
        String path = table.location() + "/data/" + java.util.UUID.randomUUID() + ".parquet";
        EncryptedOutputFile output = table.encryption().encrypt(table.io().newOutputFile(path));
        DataWriter<Record> writer = Parquet.writeData(output).forTable(table)
                .withKeyMetadata(output.keyMetadata()).createWriterFunc(GenericParquetWriter::create).build();
        try (DataWriter<Record> closeable = writer) {
            for (int id = first; id <= last; id++) {
                GenericRecord record = GenericRecord.create(SCHEMA);
                record.setField("id", id);
                closeable.write(record);
            }
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }

    private synchronized String verify() throws Exception {
        try (HiveCatalog catalog = catalog(properties())) {
            Table table = catalog.loadTable(TableIdentifier.of(DB, "encrypted_v3"));
            TableMetadata metadata = ((HasTableOperations) table).operations().current();
            check(metadata.formatVersion() == 3, "Encrypted fixture must be v3");
            check(!metadata.encryptionKeys().isEmpty(), "Encrypted table key chain is absent");
            List<List<Integer>> snapshots = new ArrayList<>();
            for (Snapshot snapshot : table.snapshots()) {
                check(snapshot.keyId() != null, "Manifest list key ID is absent");
                for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                    check(manifest.keyMetadata() != null, "Manifest encryption metadata is absent");
                }
                snapshots.add(rows(table, snapshot.snapshotId()));
            }
            check(snapshots.equals(List.of(List.of(1, 2), List.of(1, 2, 3, 4))), "Historical reference rows differ");
            Set<String> keys = new HashSet<>();
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    DataFile file = task.file();
                    check(file.keyMetadata() != null, "Data file key metadata is absent");
                    keys.add(Base64.getEncoder().encodeToString(ByteBuffers.toByteArray(file.keyMetadata())));
                    try (InputStream input = table.io().newInputFile(file.location()).newStream()) {
                        check(Arrays.equals(input.readNBytes(4), "PARE".getBytes(StandardCharsets.US_ASCII)),
                                "Data file is not native encrypted Parquet");
                    }
                }
            }
            check(keys.size() == 2, "Data files must have independent key metadata");
            check(rows(catalog.loadTable(TableIdentifier.of(DB, "plain_v3")), null)
                    .equals(List.of(1, 2, 3, 4)), "Plaintext reference rows differ");
            verifySpark(table);
            verifyMissingKms();
            verifyKmsError("credentials", "UnrecognizedClientException");
            verifyKmsError("denied", "AccessDeniedException");
            verifyWrongKey(table, metadata);
            return "{\"sdkVersion\":\"1.12.0\",\"formatVersion\":3,\"snapshots\":[[1,2],[1,2,3,4]],"
                    + "\"sparkSnapshots\":[[1,2],[1,2,3,4]],\"fileKeys\":2,"
                    + "\"encryptedManifests\":true,\"encryptedManifestLists\":true,"
                    + "\"missingKms\":\"REJECTED\",\"wrongCredentials\":\"REJECTED\","
                    + "\"denied\":\"REJECTED\",\"wrongKey\":\"REJECTED\"}";
        }
    }

    private void verifySpark(Table table) throws Exception {
        List<Long> snapshots = new ArrayList<>();
        for (Snapshot snapshot : table.snapshots()) {
            snapshots.add(snapshot.snapshotId());
        }
        String classpath = "/fixtures/jars/iceberg-spark-runtime-4.0_2.13-1.12.0.jar:"
                + "/fixtures/jars/iceberg-aws-bundle-1.12.0.jar:/tmp/encryption-classes";
        List<String> command = new ArrayList<>(List.of("/opt/spark/bin/spark-sql", "--master", "local[2]",
                "--driver-class-path", classpath, "--conf", "spark.executor.extraClassPath=" + classpath,
                "--conf", "spark.ui.enabled=false", "--conf", "spark.sql.shuffle.partitions=2",
                "--conf", "spark.sql.catalog.reference=org.apache.iceberg.spark.SparkCatalog",
                "--conf", "spark.sql.catalog.reference.type=hive"));
        for (Map.Entry<String, String> property : properties().entrySet()) {
            command.add("--conf");
            command.add("spark.sql.catalog.reference." + property.getKey() + "=" + property.getValue());
        }
        command.add("-e");
        command.add("SELECT concat('sdk:', java_method('org.apache.iceberg.IcebergBuild', 'version')); "
                + "SELECT concat('first:', CAST(id AS STRING)) FROM reference." + DB
                + ".encrypted_v3 VERSION AS OF " + snapshots.get(0) + " ORDER BY id; "
                + "SELECT concat('second:', CAST(id AS STRING)) FROM reference." + DB
                + ".encrypted_v3 VERSION AS OF " + snapshots.get(1) + " ORDER BY id");
        Path stdout = Path.of("/tmp/encryption-spark-reference.out");
        Path stderr = Path.of("/tmp/encryption-spark-reference.log");
        Process process = new ProcessBuilder(command).redirectOutput(stdout.toFile())
                .redirectError(stderr.toFile()).start();
        try {
            check(process.waitFor(90, TimeUnit.SECONDS), "Spark reference query timed out");
            check(process.exitValue() == 0, "Spark reference query failed; inspect its fixture log");
            List<String> rows = Files.readAllLines(stdout).stream().map(String::trim)
                    .filter(line -> line.startsWith("sdk:") || line.startsWith("first:") || line.startsWith("second:"))
                    .toList();
            check(rows.equals(List.of("sdk:1.12.0", "first:1", "first:2", "second:1", "second:2",
                    "second:3", "second:4")), "Spark historical reference rows differ");
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private void verifyMissingKms() throws Exception {
        Map<String, String> properties = properties();
        properties.remove(CatalogProperties.ENCRYPTION_KMS_TYPE);
        try (HiveCatalog catalog = catalog(properties)) {
            try {
                plan(catalog);
                throw new IllegalStateException("Missing KMS must fail metadata planning");
            } catch (IllegalArgumentException e) {
                check(e.getMessage().contains("without a key management client"), "Unexpected missing-KMS failure");
            }
        }
    }

    private void verifyKmsError(String mode, String expectedCode) throws Exception {
        Map<String, String> properties = properties();
        properties.put("kms.endpoint", "http://localhost:8181/kms/" + mode);
        try (HiveCatalog catalog = catalog(properties)) {
            try {
                plan(catalog);
                throw new IllegalStateException("KMS error must fail metadata planning");
            } catch (RuntimeException e) {
                Throwable cause = e;
                while (cause != null && !(cause instanceof KmsException)) {
                    cause = cause.getCause();
                }
                check(cause instanceof KmsException
                        && expectedCode.equals(((KmsException) cause).awsErrorDetails().errorCode()),
                        "Unexpected KMS error classification");
            }
        }
    }

    private void verifyWrongKey(Table table, TableMetadata metadata) throws Exception {
        try (KeyManagementClient client = EncryptionUtil.createKmsClient(properties())) {
            byte[] wrongBytes = new byte[16];
            new java.security.SecureRandom().nextBytes(wrongBytes);
            ByteBuffer wrongWrappedKey = client.wrapKey(ByteBuffer.wrap(wrongBytes), keyId);
            List<EncryptedKey> wrongKeys = new ArrayList<>();
            for (EncryptedKey key : metadata.encryptionKeys()) {
                wrongKeys.add(new BaseEncryptedKey(key.keyId(),
                        keyId.equals(key.encryptedById()) ? wrongWrappedKey : key.encryptedKeyMetadata(),
                        key.encryptedById(), key.properties()));
            }
            StandardEncryptionManager wrongManager = new StandardEncryptionManager(wrongKeys, keyId, 16, client);
            Snapshot snapshot = table.currentSnapshot();
            ManifestListFile manifestList = new ManifestListFile() {
                @Override
                public String location() {
                    return snapshot.manifestListLocation();
                }

                @Override
                public String encryptionKeyID() {
                    return snapshot.keyId();
                }

                @Override
                public ByteBuffer decryptKeyMetadata(EncryptionManager manager) {
                    return EncryptionUtil.decryptManifestListKeyMetadata(this, manager);
                }
            };
            try {
                EncryptingFileIO.combine(table.io(), wrongManager).newInputFile(manifestList);
                throw new IllegalStateException("Wrong unwrapped key must fail manifest-list decryption");
            } catch (RuntimeException e) {
                Throwable cause = e;
                while (cause != null && !(cause instanceof javax.crypto.AEADBadTagException)) {
                    cause = cause.getCause();
                }
                check(cause instanceof javax.crypto.AEADBadTagException, "Expected authenticated-decryption failure");
            }
        }
    }

    private static void plan(HiveCatalog catalog) throws Exception {
        Table table = catalog.loadTable(TableIdentifier.of(DB, "encrypted_v3"));
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            tasks.iterator().hasNext();
        }
    }

    private static List<Integer> rows(Table table, Long snapshotId) throws IOException {
        IcebergGenerics.ScanBuilder builder = IcebergGenerics.read(table);
        if (snapshotId != null) {
            builder.useSnapshot(snapshotId);
        }
        List<Integer> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = builder.build()) {
            for (Record record : records) {
                rows.add((Integer) record.getField("id"));
            }
        }
        rows.sort(Integer::compareTo);
        return rows;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void reply(HttpExchange request, int status, String response) throws IOException {
        request.getRequestBody().close();
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        request.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.1");
        request.sendResponseHeaders(status, bytes.length);
        try (java.io.OutputStream output = request.getResponseBody()) {
            output.write(bytes);
        }
    }

    /** Fixed, public fixture credentials for disposable Moto/MinIO services only. */
    public static final class FixtureCredentials implements AwsCredentialsProvider {
        public static AwsCredentialsProvider create(Map<String, String> properties) {
            return new FixtureCredentials();
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return AwsBasicCredentials.create("admin", "password");
        }
    }
}
