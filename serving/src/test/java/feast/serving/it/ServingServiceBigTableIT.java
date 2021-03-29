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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.admin.v2.stub.BigtableTableAdminStubSettings;
import com.google.cloud.bigtable.admin.v2.stub.EnhancedBigtableTableAdminStub;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import feast.common.it.DataGenerator;
import feast.common.models.FeatureV2;
import feast.proto.core.EntityProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.*;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
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
      "feast.active_store=bigtable",
      "spring.main.allow-bean-definition-overriding=true"
    })
@Testcontainers
public class ServingServiceBigTableIT extends BaseAuthIT {

  static final Map<String, String> options = new HashMap<>();
  static final String timestampPrefix = "_ts";
  static CoreSimpleAPIClient coreClient;
  static ServingServiceGrpc.ServingServiceBlockingStub servingStub;

  static final int FEAST_SERVING_PORT = 6568;

  static final String PROJECT_ID = "test-project";
  static final String INSTANCE_ID = "test-instance";
  static ManagedChannel channel;

  @ClassRule @Container
  public static DockerComposeContainer environment =
      new DockerComposeContainer(
              new File("src/test/resources/docker-compose/docker-compose-bigtable-it.yml"))
          .withExposedService(
              CORE,
              FEAST_CORE_PORT,
              Wait.forLogMessage(".*gRPC Server started.*\\n", 1)
                  .withStartupTimeout(Duration.ofMinutes(SERVICE_START_MAX_WAIT_TIME_IN_MINUTES)))
          .withExposedService(BIGTABLE, BIGTABLE_PORT);

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {
    registry.add("grpc.server.port", () -> FEAST_SERVING_PORT);
  }

  @BeforeAll
  static void globalSetup() throws IOException {
    coreClient = TestUtils.getApiClientForCore(FEAST_CORE_PORT);
    servingStub = TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);

    // Initialize BigTable Client
    BigtableDataClient client =
        BigtableDataClient.create(
            BigtableDataSettings.newBuilderForEmulator(
                    environment.getServiceHost("bigtable_1", BIGTABLE_PORT),
                    environment.getServicePort("bigtable_1", BIGTABLE_PORT))
                .setProjectId(PROJECT_ID)
                .setInstanceId(INSTANCE_ID)
                .build());

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

    // Serving
    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    ServingAPIProto.FeatureReferenceV2 feature2Reference =
        DataGenerator.createFeatureReference("rides", "trip_distance");
    ServingAPIProto.FeatureReferenceV2 feature3Reference =
        DataGenerator.createFeatureReference("rides", "trip_empty");
    ServingAPIProto.FeatureReferenceV2 feature4Reference =
        DataGenerator.createFeatureReference("rides", "trip_wrong_type");

    // Event Timestamp
    String eventTimestampKey = timestampPrefix + ":" + ridesFeatureTableName;
    Timestamp eventTimestampValue = Timestamp.newBuilder().setSeconds(100).build();

    String btTableName = String.format("%s__%s", projectName, driverEntityName);
    String featureTableName = "rides";
    String metadataColumnFamily = "metadata";
    ImmutableList<String> columnFamilies = ImmutableList.of(featureTableName, metadataColumnFamily);
    String emptyQualifier = "";

    Schema ftSchema =
        SchemaBuilder.record("DriverData")
            .namespace(featureTableName)
            .fields()
            .requiredInt(feature1Reference.getName())
            .requiredDouble(feature2Reference.getName())
            .nullableString(feature3Reference.getName(), "null")
            .requiredString(feature4Reference.getName())
            .endRecord();
    byte[] schemaReference =
        Hashing.murmur3_32().hashBytes(ftSchema.toString().getBytes()).asBytes();

    // Entity-Feature Row
    GenericRecord record =
        new GenericRecordBuilder(ftSchema)
            .set("trip_cost", 5)
            .set("trip_distance", 3.5)
            .set("trip_empty", null)
            .set("trip_wrong_type", "test")
            .build();
    byte[] avroSerializedFeatures = recordToAvro(record, ftSchema);

    byte[] entityFeatureKey =
        String.valueOf(DataGenerator.createInt64Value(1).getInt64Val()).getBytes();

    ByteArrayOutputStream entityFeatureOutputStream = new ByteArrayOutputStream();
    entityFeatureOutputStream.write(schemaReference);
    entityFeatureOutputStream.write("".getBytes());
    entityFeatureOutputStream.write(avroSerializedFeatures);
    byte[] entityFeatureValue = entityFeatureOutputStream.toByteArray();

    // SchemaKey
    ByteArrayOutputStream concatOutputStream = new ByteArrayOutputStream();
    concatOutputStream.write("schema#".getBytes());
    concatOutputStream.write(schemaReference);
    byte[] schemaKey = concatOutputStream.toByteArray();

    String endpoint =
        environment.getServiceHost("bigtable_1", BIGTABLE_PORT)
            + ":"
            + environment.getServicePort("bigtable_1", BIGTABLE_PORT);
    // ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
    channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
    TransportChannelProvider channelProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    NoCredentialsProvider credentialsProvider = NoCredentialsProvider.create();

    createTable(channelProvider, credentialsProvider, btTableName, columnFamilies);

    // Update Entity-Feature Row
    client.mutateRow(
        RowMutation.create(btTableName, ByteString.copyFrom(entityFeatureKey))
            .setCell(
                featureTableName,
                ByteString.copyFrom(emptyQualifier.getBytes()),
                ByteString.copyFrom(entityFeatureValue)));

    // Update Schema Row
    client.mutateRow(
        RowMutation.create(btTableName, ByteString.copyFrom(schemaKey))
            .setCell(
                metadataColumnFamily,
                ByteString.copyFrom("avro".getBytes()),
                ByteString.copyFrom(ftSchema.toString().getBytes())));

    // set up options for call credentials
    options.put("oauth_url", TOKEN_URL);
    options.put(CLIENT_ID, CLIENT_ID);
    options.put(CLIENT_SECRET, CLIENT_SECRET);
    options.put("jwkEndpointURI", JWK_URI);
    options.put("audience", AUDIENCE);
    options.put("grant_type", GRANT_TYPE);
  }

  @AfterAll
  static void tearDown() {
    ((ManagedChannel) servingStub.getChannel()).shutdown();
    channel.shutdown();
  }

  private static void createTable(
      TransportChannelProvider channelProvider,
      CredentialsProvider credentialsProvider,
      String tableName,
      List<String> columnFamilies)
      throws IOException {
    EnhancedBigtableTableAdminStub stub =
        EnhancedBigtableTableAdminStub.createEnhanced(
            BigtableTableAdminStubSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build());

    try (BigtableTableAdminClient client =
        BigtableTableAdminClient.create(PROJECT_ID, INSTANCE_ID, stub)) {
      CreateTableRequest createTableRequest = CreateTableRequest.of(tableName);
      for (String columnFamily : columnFamilies) {
        createTableRequest.addFamily(columnFamily);
      }
      Table table = client.createTable(createTableRequest);
    }
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
  public void shouldRegisterAndGetOnlineFeaturesWithNotFound() {
    // getOnlineFeatures Information
    String projectName = "default";
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();

    // Instantiate EntityRows
    ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(entityName, DataGenerator.createInt64Value(1), 100);
    ImmutableList<ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow> entityRows =
        ImmutableList.of(entityRow1);

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 featureReference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    ServingAPIProto.FeatureReferenceV2 notFoundFeatureReference =
        DataGenerator.createFeatureReference("rides", "trip_transaction");

    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(featureReference, notFoundFeatureReference);

    // Build GetOnlineFeaturesRequestV2
    ServingAPIProto.GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    ServingAPIProto.GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(featureReference),
            DataGenerator.createInt64Value(5),
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            DataGenerator.createEmptyValue());

    ImmutableMap<String, ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(featureReference),
            ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(notFoundFeatureReference),
            ServingAPIProto.GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND);

    ServingAPIProto.GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        ServingAPIProto.GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<ServingAPIProto.GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }

  @TestConfiguration
  public static class TestConfig {
    @Bean
    public BigtableDataClient bigtableClient() throws IOException {
      return BigtableDataClient.create(
          BigtableDataSettings.newBuilderForEmulator(
                  environment.getServiceHost("bigtable_1", BIGTABLE_PORT),
                  environment.getServicePort("bigtable_1", BIGTABLE_PORT))
              .setProjectId(PROJECT_ID)
              .setInstanceId(INSTANCE_ID)
              .build());
    }
  }
}
