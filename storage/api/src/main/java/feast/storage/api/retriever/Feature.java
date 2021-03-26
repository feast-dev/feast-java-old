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
package feast.storage.api.retriever;

import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.types.ValueProto;
import feast.proto.types.ValueProto.Value;

public interface Feature {
  Value getFeatureValue(ValueProto.ValueType.Enum valueType);

  FeatureReferenceV2 getFeatureReference();

  Timestamp getEventTimestamp();

  Boolean isSameFeatureSpec(ValueProto.ValueType.Enum anEnum);
}
