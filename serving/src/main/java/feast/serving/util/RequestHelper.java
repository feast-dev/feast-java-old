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
package feast.serving.util;

import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2;

public class RequestHelper {

  public static void validateOnlineRequest(GetOnlineFeaturesRequestV2 request) {
    // All EntityRows should not be empty
    if (request.getEntityRowsCount() <= 0) {
      throw new IllegalArgumentException("Entity value must be provided");
    }
    // All FeatureReferences should have FeatureTable name and Feature name
    for (FeatureReferenceV2 featureReference : request.getFeaturesList()) {
      validateOnlineRequestFeatureReference(featureReference);
    }
  }

  public static void validateOnlineRequestFeatureReference(FeatureReferenceV2 featureReference) {
    if (featureReference.getFeatureTable().isEmpty()) {
      throw new IllegalArgumentException("FeatureTable name must be provided in FeatureReference");
    }
    if (featureReference.getName().isEmpty()) {
      throw new IllegalArgumentException("Feature name must be provided in FeatureReference");
    }
  }
}
