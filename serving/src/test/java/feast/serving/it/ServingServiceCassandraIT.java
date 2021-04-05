/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.serving.it;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import feast.common.it.DataGenerator;
import feast.proto.core.EntityProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.ValueProto;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "feast.core-cache-refresh-interval=1",
      "feast.active_store=cassandra",
      "spring.main.allow-bean-definition-overriding=true"
    })
@Testcontainers
public class ServingServiceCassandraIT extends BaseAuthIT {

  static final Map<String, String> options = new HashMap<>();
  static CoreSimpleAPIClient coreClient;
  static ServingServiceGrpc.ServingServiceBlockingStub servingStub;

  static CqlSession cqlSession;
  //    static Session session;
  static final int FEAST_SERVING_PORT = 6570;

  static final ServingAPIProto.FeatureReferenceV2 feature1Reference =
      DataGenerator.createFeatureReference("rides", "trip_cost");
  static final ServingAPIProto.FeatureReferenceV2 feature2Reference =
      DataGenerator.createFeatureReference("rides", "trip_distance");
  static final ServingAPIProto.FeatureReferenceV2 feature3Reference =
      DataGenerator.createFeatureReference("rides", "trip_empty");
  static final ServingAPIProto.FeatureReferenceV2 feature4Reference =
      DataGenerator.createFeatureReference("rides", "trip_wrong_type");
  static final String KEYSPACE = "feast";

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-cassandra-it.yml"))
          .withExposedService(
              CORE,
              FEAST_CORE_PORT,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(SERVICE_START_MAX_WAIT_TIME_IN_MINUTES)))
          .withExposedService(CASSANDRA, CASSANDRA_PORT);

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {
    registry.add("grpc.server.port", () -> FEAST_SERVING_PORT);
  }

  @BeforeAll
  static void globalSetup() throws IOException {
    coreClient = TestUtils.getApiClientForCore(FEAST_CORE_PORT);
    servingStub = TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);

    cqlSession =
        CqlSession.builder()
            .addContactPoint(
                new InetSocketAddress(
                    environment.getServiceHost("cassandra_1", CASSANDRA_PORT),
                    environment.getServicePort("cassandra_1", CASSANDRA_PORT)))
            .withLocalDatacenter(CASSANDRA_DATACENTER)
            .build();

    /** Feast resource creation Workflow */
    String projectName = "default";
    // Apply Entity (driver_id)
    String driverEntityName = "driver_id";
    String driverEntityDescription = "My driver id";
    ValueProto.ValueType.Enum driverEntityType = ValueProto.ValueType.Enum.INT64;
    EntityProto.EntitySpecV2 driverEntitySpec =
        EntityProto.EntitySpecV2.newBuilder()
            .setName(driverEntityName)
            .setDescription(driverEntityDescription)
            .setValueType(driverEntityType)
            .build();
    TestUtils.applyEntity(coreClient, projectName, driverEntitySpec);

    // Apply Entity (merchant_id)
    String merchantEntityName = "merchant_id";
    String merchantEntityDescription = "My driver id";
    ValueProto.ValueType.Enum merchantEntityType = ValueProto.ValueType.Enum.INT64;
    EntityProto.EntitySpecV2 merchantEntitySpec =
        EntityProto.EntitySpecV2.newBuilder()
            .setName(merchantEntityName)
            .setDescription(merchantEntityDescription)
            .setValueType(merchantEntityType)
            .build();
    TestUtils.applyEntity(coreClient, projectName, merchantEntitySpec);

    // Apply FeatureTable (rides)
    String ridesFeatureTableName = "rides";
    ImmutableList<String> ridesEntities = ImmutableList.of(driverEntityName);
    ImmutableMap<String, ValueProto.ValueType.Enum> ridesFeatures =
        ImmutableMap.of(
            "trip_cost",
            ValueProto.ValueType.Enum.INT64,
            "trip_distance",
            ValueProto.ValueType.Enum.DOUBLE,
            "trip_empty",
            ValueProto.ValueType.Enum.DOUBLE,
            "trip_wrong_type",
            ValueProto.ValueType.Enum.STRING);
    TestUtils.applyFeatureTable(
        coreClient, projectName, ridesFeatureTableName, ridesEntities, ridesFeatures, 7200);

    cqlSession.execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE));

    cqlSession.execute(
        String.format(
            "CREATE KEYSPACE %s WITH replication = \n"
                + "{'class':'SimpleStrategy','replication_factor':'1'};",
            KEYSPACE));

    ImmutableList.of(driverEntityName, merchantEntityName);
    String cassandraTableName = String.format("%s__%s", projectName, driverEntityName);

    cqlSession.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (key BLOB, schema_ref BLOB, PRIMARY KEY (key));",
            KEYSPACE, cassandraTableName));

    // Add column families
    cqlSession.execute(
        String.format("ALTER TABLE %s.%s ADD (rides BLOB)", KEYSPACE, cassandraTableName));

    /** Single Entity Ingestion Workflow */
    Schema ftSchema =
        SchemaBuilder.record("DriverData")
            .namespace(ridesFeatureTableName)
            .fields()
            .requiredInt(feature1Reference.getName())
            .requiredDouble(feature2Reference.getName())
            .nullableString(feature3Reference.getName(), "null")
            .requiredString(feature4Reference.getName())
            .endRecord();
    byte[] schemaReference =
        Hashing.murmur3_32().hashBytes(ftSchema.toString().getBytes()).asBytes();

    GenericRecord record =
        new GenericRecordBuilder(ftSchema)
            .set("trip_cost", 5)
            .set("trip_distance", 3.5)
            .set("trip_empty", null)
            .set("trip_wrong_type", "test")
            .build();
    byte[] entityFeatureKey =
        String.valueOf(DataGenerator.createInt64Value(1).getInt64Val()).getBytes();
    byte[] entityFeatureValue = createEntityValue(ftSchema, schemaReference, record);
    byte[] schemaKey = createSchemaKey(schemaReference);

    PreparedStatement statement =
        cqlSession.prepare(
            String.format(
                "INSERT INTO %s.%s (key, schema_ref, rides) VALUES (?, ?, ?)",
                KEYSPACE, cassandraTableName));
    cqlSession.execute(
        statement.bind(
            ByteBuffer.wrap(entityFeatureKey),
            ByteBuffer.wrap(schemaKey),
            ByteBuffer.wrap(entityFeatureValue)));
  }

  private static byte[] createSchemaKey(byte[] schemaReference) throws IOException {
    ByteArrayOutputStream concatOutputStream = new ByteArrayOutputStream();
    concatOutputStream.write(schemaReference);
    byte[] schemaKey = concatOutputStream.toByteArray();

    return schemaKey;
  }

  private static byte[] createEntityValue(
      Schema schema, byte[] schemaReference, GenericRecord record) throws IOException {
    // Entity-Feature Row
    byte[] avroSerializedFeatures = recordToAvro(record, schema);

    ByteArrayOutputStream concatOutputStream = new ByteArrayOutputStream();
    concatOutputStream.write(schemaReference);
    concatOutputStream.write("".getBytes());
    concatOutputStream.write(avroSerializedFeatures);
    byte[] entityFeatureValue = concatOutputStream.toByteArray();

    return entityFeatureValue;
  }

  private static byte[] recordToAvro(GenericRecord datum, Schema schema) throws IOException {
    GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    writer.write(datum, encoder);
    encoder.flush();

    return output.toByteArray();
  }

  @Test
  public void shouldRegisterSingleEntityAndGetOnlineFeatures() {
    String projectName = "default";
    String entityName = "driver_id";
    String cassandraTableName = String.format("%s__%s", projectName, entityName);
    byte[] entityFeatureKey =
        String.valueOf(DataGenerator.createInt64Value(1).getInt64Val()).getBytes();
    String featureTableName = "rides";

    BoundStatement statement =
        cqlSession
            .prepare(
                String.format("SELECT * FROM %s.%s WHERE key = ?", KEYSPACE, cassandraTableName))
            .bind(ByteBuffer.wrap(entityFeatureKey));
    Row row = cqlSession.execute(statement).one();

    assertEquals(ByteBuffer.wrap(entityFeatureKey), row.getByteBuffer("key"));
  }
}
