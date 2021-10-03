/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.protobuf.Timestamp;
import feast.common.it.DataGenerator;
import feast.common.models.FeatureV2;
import feast.proto.core.EntityProto;
import feast.proto.serving.ServingAPIProto;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesResponse;
import feast.proto.serving.ServingServiceGrpc;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import io.grpc.ManagedChannel;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;

@ActiveProfiles("it")
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
      "feast.registry:=./serving/src/test/resources/feast_project/data/registry.db",
      "feast.stores[0].config.port=6389"
    })
@Testcontainers
public class ServingServiceFeast10IT extends BaseAuthIT {

  static final Map<String, String> options = new HashMap<>();
  static final String timestampPrefix = "_ts";
  static CoreSimpleAPIClient coreClient;
  static ServingServiceGrpc.ServingServiceBlockingStub servingStub;
  static RedisCommands<byte[], byte[]> syncCommands;

  static final int FEAST_SERVING_PORT = 6568;
  @LocalServerPort private int metricsPort;

  @DynamicPropertySource
  static void initialize(DynamicPropertyRegistry registry) {
    registry.add("grpc.server.port", () -> FEAST_SERVING_PORT);
  }

  @BeforeAll
  static void globalSetup() {
    servingStub = TestUtils.getServingServiceStub(false, FEAST_SERVING_PORT, null);

    RedisClient redisClient =
        RedisClient.create(new RedisURI("localhost", 6389, Duration.ofMillis(2000)));
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    syncCommands = connection.sync();

    String projectName = "default";
    // Apply Entity
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();
    String description = "My driver id";
    ValueProto.ValueType.Enum entityType = ValueProto.ValueType.Enum.INT64;
    EntityProto.EntitySpecV2 entitySpec =
        EntityProto.EntitySpecV2.newBuilder()
            .setName(entityName)
            .setDescription(description)
            .setValueType(entityType)
            .build();
    TestUtils.applyEntity(coreClient, projectName, entitySpec);

    // Apply FeatureTable
    String featureTableName = "rides";

    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        DataGenerator.createFeatureReference("rides", "trip_cost");
    ServingAPIProto.FeatureReferenceV2 feature2Reference =
        DataGenerator.createFeatureReference("rides", "trip_distance");
    ServingAPIProto.FeatureReferenceV2 feature3Reference =
        DataGenerator.createFeatureReference("rides", "trip_empty");
    ServingAPIProto.FeatureReferenceV2 feature4Reference =
        DataGenerator.createFeatureReference("rides", "trip_wrong_type");

    // Event Timestamp
    String eventTimestampKey = timestampPrefix + ":" + featureTableName;
    Timestamp eventTimestampValue = Timestamp.newBuilder().setSeconds(100).build();

    // Serialize Redis Key with Entity i.e <default_driver_id_1>
    RedisProto.RedisKeyV2 redisKey =
        RedisProto.RedisKeyV2.newBuilder()
            .setProject(projectName)
            .addEntityNames(entityName)
            .addEntityValues(entityValue)
            .build();

    ImmutableMap<ServingAPIProto.FeatureReferenceV2, ValueProto.Value> featureReferenceValueMap =
        ImmutableMap.of(
            feature1Reference,
            DataGenerator.createInt64Value(42),
            feature2Reference,
            DataGenerator.createDoubleValue(42.2),
            feature3Reference,
            DataGenerator.createEmptyValue(),
            feature4Reference,
            DataGenerator.createDoubleValue(42.2));

    // Insert timestamp into Redis and isTimestampMap only once
    syncCommands.hset(
        redisKey.toByteArray(), eventTimestampKey.getBytes(), eventTimestampValue.toByteArray());
    featureReferenceValueMap.forEach(
        (featureReference, featureValue) -> {
          // Murmur hash Redis Feature Field i.e murmur(<rides:trip_distance>)
          String delimitedFeatureReference =
              featureReference.getFeatureTable() + ":" + featureReference.getName();
          byte[] featureReferenceBytes =
              Hashing.murmur3_32()
                  .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
                  .asBytes();
          // Insert features into Redis
          syncCommands.hset(
              redisKey.toByteArray(), featureReferenceBytes, featureValue.toByteArray());
        });

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
  }

  @Test
  public void shouldRegisterAndGetOnlineFeatures() {
    // getOnlineFeatures Information
    String projectName = "feast_project";
    String entityName = "driver_id";
    ValueProto.Value entityValue = ValueProto.Value.newBuilder().setInt64Val(1).build();

    // Instantiate EntityRows
    final Timestamp timestamp = Timestamp.getDefaultInstance();
    GetOnlineFeaturesRequestV2.EntityRow entityRow1 =
        DataGenerator.createEntityRow(
            entityName, DataGenerator.createInt64Value(1001), timestamp.getSeconds());
    ImmutableList<GetOnlineFeaturesRequestV2.EntityRow> entityRows = ImmutableList.of(entityRow1);

    // Instantiate FeatureReferences
    ServingAPIProto.FeatureReferenceV2 feature1Reference =
        DataGenerator.createFeatureReference("driver_hourly_stats", "conv_rate");
    ServingAPIProto.FeatureReferenceV2 feature2Reference =
        DataGenerator.createFeatureReference("driver_hourly_stats", "acc_rate");
    ServingAPIProto.FeatureReferenceV2 feature3Reference =
        DataGenerator.createFeatureReference("driver_hourly_stats", "avg_daily_trips");
    ImmutableList<ServingAPIProto.FeatureReferenceV2> featureReferences =
        ImmutableList.of(feature1Reference, feature2Reference, feature3Reference);

    // Build GetOnlineFeaturesRequestV2
    GetOnlineFeaturesRequestV2 onlineFeatureRequest =
        TestUtils.createOnlineFeatureRequest(projectName, featureReferences, entityRows);
    GetOnlineFeaturesResponse featureResponse =
        servingStub.getOnlineFeaturesV2(onlineFeatureRequest);

    ImmutableMap<String, ValueProto.Value> expectedValueMap =
        ImmutableMap.of(
            entityName,
            entityValue,
            FeatureV2.getFeatureStringRef(feature1Reference),
            DataGenerator.createInt64Value(42));

    ImmutableMap<String, GetOnlineFeaturesResponse.FieldStatus> expectedStatusMap =
        ImmutableMap.of(
            entityName,
            GetOnlineFeaturesResponse.FieldStatus.PRESENT,
            FeatureV2.getFeatureStringRef(feature1Reference),
            GetOnlineFeaturesResponse.FieldStatus.PRESENT);

    GetOnlineFeaturesResponse.FieldValues expectedFieldValues =
        GetOnlineFeaturesResponse.FieldValues.newBuilder()
            .putAllFields(expectedValueMap)
            .putAllStatuses(expectedStatusMap)
            .build();
    ImmutableList<GetOnlineFeaturesResponse.FieldValues> expectedFieldValuesList =
        ImmutableList.of(expectedFieldValues);

    assertEquals(expectedFieldValuesList, featureResponse.getFieldValuesList());
  }
}
