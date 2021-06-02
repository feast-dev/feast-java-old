/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.serving.specs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import feast.proto.core.CoreServiceProto.GetFeatureTableRequest;
import feast.proto.core.CoreServiceProto.GetFeatureTableResponse;
import feast.proto.core.CoreServiceProto.ListFeatureTablesRequest;
import feast.proto.core.CoreServiceProto.ListFeatureTablesResponse;
import feast.proto.core.FeatureTableProto.FeatureTable;
import feast.proto.core.RegistryProto.Registry;
import feast.serving.exception.SpecRetrievalException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.slf4j.Logger;

/** Client for interfacing with specs in Feast Core. */
public class RegistrySpecService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(RegistrySpecService.class);
  private Blob blob;
  private Storage storage;
  private String bucketName;
  private String objectName;

  public RegistrySpecService(String bucket, String object) {
    bucketName = bucket;
    objectName = object;
    storage = StorageOptions.getDefaultInstance().getService();
  }

  public ListFeatureTablesResponse listFeatureTables(
      ListFeatureTablesRequest listFeatureTablesRequest) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    blob = storage.get(BlobId.of(bucketName, objectName));
    blob.downloadTo(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Registry registry = null;
    try {
      registry = Registry.parseFrom(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve registry", e);
    }
    ListFeatureTablesResponse.Builder response = ListFeatureTablesResponse.newBuilder();
    response.addAllTables(registry.getFeatureTablesList());
    return response.build();
  }

  public GetFeatureTableResponse getFeatureTable(GetFeatureTableRequest getFeatureTableRequest) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    blob = storage.get(BlobId.of(bucketName, objectName));
    blob.downloadTo(outputStream);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Registry registry = null;
    try {
      registry = Registry.parseFrom(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Unable to retrieve registry", e);
    }
    GetFeatureTableResponse.Builder response = GetFeatureTableResponse.newBuilder();
    String error = "";
    for (FeatureTable table : registry.getFeatureTablesList()) {
      if (table.getSpec().getProject().equals(getFeatureTableRequest.getProject())
          && table.getSpec().getName().equals(getFeatureTableRequest.getName())) {
        return response.setTable(table).build();
      } else {
        error = error + " next " + table.getSpec().getProject() + " " + table.getSpec().getName();
      }
    }
    throw new SpecRetrievalException(
        String.format(
                "Unable to find FeatureTable %s/%s",
                getFeatureTableRequest.getProject(), getFeatureTableRequest.getName())
            + error);
  }
}
