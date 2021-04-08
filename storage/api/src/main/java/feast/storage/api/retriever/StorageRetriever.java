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
package feast.storage.api.retriever;

import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import java.util.List;
import java.util.stream.Collectors;

public class StorageRetriever {

  /**
   * Generate name of Cassandra table in the form of <feastProject>__<entityNames>
   *
   * @param project Name of Feast project
   * @param entityNames List of entities used in retrieval call
   * @return Name of Cassandra table
   */
  protected String getTableName(String project, List<String> entityNames) {
    return String.format("%s__%s", project, entityNames.stream().collect(Collectors.joining("__")));
  }

  /**
   * Convert Entity value from Feast valueType to String type. Currently only supports STRING_VAL,
   * INT64_VAL, INT32_VAL and BYTES_VAL.
   *
   * @param v Entity value of Feast valueType
   * @return String representation of Entity value
   */
  protected String valueToString(ValueProto.Value v) {
    String stringRepr;
    switch (v.getValCase()) {
      case STRING_VAL:
        stringRepr = v.getStringVal();
        break;
      case INT64_VAL:
        stringRepr = String.valueOf(v.getInt64Val());
        break;
      case INT32_VAL:
        stringRepr = String.valueOf(v.getInt32Val());
        break;
      case BYTES_VAL:
        stringRepr = v.getBytesVal().toString();
        break;
      default:
        throw new RuntimeException("Type is not supported to be entity");
    }

    return stringRepr;
  }

  /**
   * Retrieve Cassandra table column families based on FeatureTable names.
   *
   * @param featureReferences List of feature references of features in retrieval call
   * @return List of String of FeatureTable names
   */
  protected List<String> getColumnFamilies(
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    return featureReferences.stream()
        .map(ServingAPIProto.FeatureReferenceV2::getFeatureTable)
        .distinct()
        .collect(Collectors.toList());
  }
}
