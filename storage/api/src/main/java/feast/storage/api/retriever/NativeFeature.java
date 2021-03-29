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

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;

public class NativeFeature implements Feature {
  private final ServingAPIProto.FeatureReferenceV2 featureReference;

  private final Timestamp eventTimestamp;

  private final Object featureValue;

  public NativeFeature(
      ServingAPIProto.FeatureReferenceV2 featureReference,
      Timestamp eventTimestamp,
      Object featureValue) {
    this.featureReference = featureReference;
    this.eventTimestamp = eventTimestamp;
    this.featureValue = featureValue;
  }

  @Override
  public ValueProto.Value getFeatureValue(ValueProto.ValueType.Enum valueType) {
    ValueProto.Value finalValue;

    // Check error for type casting
    // Add various type cases
    switch (valueType) {
      case STRING:
        finalValue = ValueProto.Value.newBuilder().setStringVal((String) featureValue).build();
        break;
      case INT32:
        finalValue = ValueProto.Value.newBuilder().setInt32Val((Integer) featureValue).build();
        break;
      case INT64:
        finalValue = ValueProto.Value.newBuilder().setInt64Val((Integer) featureValue).build();
        break;
      case DOUBLE:
        finalValue = ValueProto.Value.newBuilder().setDoubleVal((Long) featureValue).build();
        break;
      case FLOAT:
        finalValue = ValueProto.Value.newBuilder().setFloatVal((Long) featureValue).build();
        break;
      case BYTES:
        finalValue = ValueProto.Value.newBuilder().setBytesVal((ByteString) featureValue).build();
        break;
      case BOOL:
        finalValue = ValueProto.Value.newBuilder().setBoolVal((Boolean) featureValue).build();
        break;
      default:
        throw new RuntimeException("FeatureType is not supported");
    }
    return finalValue;
  }

  @Override
  public ServingAPIProto.FeatureReferenceV2 getFeatureReference() {
    return this.featureReference;
  }

  @Override
  public Timestamp getEventTimestamp() {
    return this.eventTimestamp;
  }

  @Override
  public Boolean isSameFeatureSpec(ValueProto.ValueType.Enum valueType) {
    // TODO: Implement similar logic check in ProtoFeature (but for Avro types)
    return null;
  }
}
