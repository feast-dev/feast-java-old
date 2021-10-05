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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import com.gojek.feast.GrpcMock;
import feast.proto.core.CoreServiceGrpc;
import java.util.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

public class CoreClientTest {

  private final CoreServiceGrpc.CoreServiceImplBase coreMock =
      mock(CoreServiceGrpc.CoreServiceImplBase.class, delegatesTo(new CoreServiceImplMock()));

  private CoreClient client;

  @Before
  public void setup() throws Exception {
    GrpcMock grpcMock = new GrpcMock(this.coreMock);
    this.client = new CoreClient(grpcMock.getChannel(), Optional.empty());
  }

  @BeforeEach
  void setUp() {
    CoreServiceImplMock.clear();
  }

  @Test
  public void testProject() {
    Assertions.assertEquals(CoreServiceImplMock.PROJECT, client.listProjects().get(0));
  }

  @Test
  public void testEntity() {
    final String name = "entity";
    Entity.Spec spec =
        Entity.Spec.getBuilder(CoreServiceImplMock.PROJECT, name)
            .setValue(ValueType.BOOL)
            .setDescription("description")
            .build();
    client.apply(spec);
    Assertions.assertEquals(
        spec.toProto(),
        client.getEntity(CoreServiceImplMock.PROJECT, name).get().getSpec().toProto());
    spec.getLabels().put("l1", "v1");
    client.apply(spec);
    Assertions.assertTrue(
        client
            .getEntity(CoreServiceImplMock.PROJECT, name)
            .get()
            .getSpec()
            .getLabels()
            .containsKey("l1"));
    Assertions.assertEquals(1, client.listEntities(CoreServiceImplMock.PROJECT).size());
  }

  @Test
  public void testFeatureTable() {
    final String name = "featureTable";
    FeatureTable.Spec spec =
        FeatureTable.Spec.getBuilder(CoreServiceImplMock.PROJECT, name)
            .addEntity("entity")
            .addLabel("key", "value")
            .build();
    client.apply(spec);
    Assertions.assertEquals(
        spec.toProto(),
        client.getFeatureTable(CoreServiceImplMock.PROJECT, name).get().getSpec().toProto());
    spec.getFeatures().add(Feature.getBuilder("feature", ValueType.FLOAT).build());
    client.apply(spec);
    Assertions.assertTrue(
        client.getFeatureTable(CoreServiceImplMock.PROJECT, name).get().getSpec().getFeatures()
            .stream()
            .anyMatch(f -> f.getName().equals("feature")));
    Assertions.assertEquals(1, client.listFeatureTables(CoreServiceImplMock.PROJECT).size());
    Assertions.assertEquals(1, client.listFeatures(CoreServiceImplMock.PROJECT).size());
    client.deleteFeatureTable(CoreServiceImplMock.PROJECT, name);
    Assertions.assertTrue(client.listFeatureTables(CoreServiceImplMock.PROJECT).isEmpty());
  }
}
