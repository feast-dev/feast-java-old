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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import feast.proto.core.EntityProto;
import feast.proto.core.EntityProto.Entity;
import feast.proto.core.FeatureTableProto;
import feast.proto.core.FeatureTableProto.FeatureTable;
import feast.proto.core.RegistryProto.Registry;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RegistrySimpleAPIClient {
  private Blob blob;
  private BlobInfo blobInfo;
  private Storage storage;
  private String bucketName;
  private String objectName;

  public RegistrySimpleAPIClient(String bucket, String object) {
    storage = StorageOptions.getDefaultInstance().getService();
    bucketName = bucket;
    objectName = object;
    blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, objectName)).build();
  }

  public void simpleApplyEntity(String projectName, EntityProto.EntitySpecV2 entitySpec) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    blob = storage.get(bucketName, objectName);
    blob.downloadTo(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Registry registry = null;
    try {
      registry = Registry.parseFrom(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve registry", e);
    }
    entitySpec = entitySpec.toBuilder().setProject(projectName).build();
    int idx = 0;
    for (Entity entity : registry.getEntitiesList()) {
      if (entity.getSpec().getProject().equals(projectName)
          && entity.getSpec().getName().equals(entitySpec.getName())) {
        registry =
            registry.toBuilder().setEntities(idx, entity.toBuilder().setSpec(entitySpec)).build();
        blob = storage.create(blobInfo, registry.toByteArray());
        return;
      }
      idx++;
    }
    registry = registry.toBuilder().addEntities(Entity.newBuilder().setSpec(entitySpec)).build();
    blob = storage.create(blobInfo, registry.toByteArray());
    return;
  }

  public Entity getEntity(String projectName, String name) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    blob = storage.get(bucketName, objectName);
    blob.downloadTo(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Registry registry = null;
    try {
      registry = Registry.parseFrom(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve registry", e);
    }
    for (Entity entity : registry.getEntitiesList()) {
      if (entity.getSpec().getProject().equals(projectName)
          && entity.getSpec().getName().equals(name)) {
        return entity;
      } else {
      }
    }
    throw new RuntimeException(
        String.format("Entity with name %s and project %s not found", name, projectName));
  }

  public void simpleApplyFeatureTable(
      String projectName, FeatureTableProto.FeatureTableSpec featureTableSpec) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    blob = storage.get(bucketName, objectName);
    blob.downloadTo(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Registry registry = null;
    try {
      registry = Registry.parseFrom(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve registry", e);
    }
    featureTableSpec = featureTableSpec.toBuilder().setProject(projectName).build();
    int idx = 0;
    for (FeatureTable featureTable : registry.getFeatureTablesList()) {
      if (featureTable.getSpec().getProject().equals(projectName)
          && featureTable.getSpec().getName().equals(featureTableSpec.getName())) {
        registry =
            registry
                .toBuilder()
                .setFeatureTables(idx, featureTable.toBuilder().setSpec(featureTableSpec))
                .build();
        blob = storage.create(blobInfo, registry.toByteArray());
        return;
      }
      idx++;
    }
    registry =
        registry
            .toBuilder()
            .addFeatureTables(FeatureTable.newBuilder().setSpec(featureTableSpec))
            .build();
    blob = storage.create(blobInfo, registry.toByteArray());
    return;
  }

  public FeatureTable simpleGetFeatureTable(String projectName, String name) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    blob = storage.get(bucketName, objectName);
    blob.downloadTo(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Registry registry = null;
    try {
      registry = Registry.parseFrom(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve registry", e);
    }
    for (FeatureTable featureTable : registry.getFeatureTablesList()) {
      if (featureTable.getSpec().getProject().equals(projectName)
          && featureTable.getSpec().getName().equals(name)) {
        return featureTable;
      }
    }
    throw new RuntimeException(
        String.format("FeatureTable with name %s and project %s not found", name, projectName));
  }
}
