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
package com.gojek.feast.core;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureTableProto;
import feast.proto.types.ValueProto;
import org.junit.Assert;
import org.junit.Test;

public class ProtobufSerializationTest {
  public static final Timestamp TIMESTAMP = Timestamp.newBuilder().setSeconds(100000).build();

  @Test
  public void testValueType() {
    for (Descriptors.EnumValueDescriptor value :
        ValueProto.ValueType.Enum.getDescriptor().getValues()) {
      ValueType valueType = ValueType.valueOf(value.getName());
      Assert.assertNotNull("ValueType enum=" + value.getName() + " Is missing.", valueType);
      Assert.assertEquals(valueType.getNumber(), value.getNumber());
    }
  }

  @Test
  public void testEntity() {
    EntityProto.Entity protoEntity =
        EntityProto.Entity.newBuilder()
            .setMeta(
                EntityProto.EntityMeta.newBuilder()
                    .setCreatedTimestamp(TIMESTAMP)
                    .setLastUpdatedTimestamp(TIMESTAMP))
            .setSpec(
                EntityProto.EntitySpecV2.newBuilder()
                    .setName("name")
                    .setDescription("description")
                    .setValueType(ValueProto.ValueType.Enum.BOOL_LIST)
                    .putLabels("key", "val"))
            .build();
    Entity entity = new Entity("project", protoEntity);
    Assert.assertEquals(protoEntity, entity.toProto());
  }

  @Test
  public void testFeature() {
    FeatureProto.FeatureSpecV2 featureProto =
        FeatureProto.FeatureSpecV2.newBuilder()
            .setName("name")
            .setValueType(ValueProto.ValueType.Enum.BOOL)
            .putLabels("key", "val")
            .build();
    Feature feature = new Feature(featureProto);
    Assert.assertEquals(featureProto, feature.toProto());
  }

  @Test
  public void testFeatureTable() {
    FeatureTableProto.FeatureTable featureTableProto =
        FeatureTableProto.FeatureTable.newBuilder()
            .setMeta(
                FeatureTableProto.FeatureTableMeta.newBuilder()
                    .setCreatedTimestamp(TIMESTAMP)
                    .setLastUpdatedTimestamp(TIMESTAMP))
            .setSpec(
                FeatureTableProto.FeatureTableSpec.newBuilder()
                    .setName("name")
                    .setMaxAge(Duration.newBuilder().setSeconds(10).setNanos(10))
                    .addEntities("entity")
                    .putLabels("key", "val")
                    .addFeatures(FeatureProto.FeatureSpecV2.getDefaultInstance()))
            .build();
    FeatureTable featureTable = new FeatureTable("project", featureTableProto);
    Assert.assertEquals(featureTableProto, featureTable.toProto());
  }
}
