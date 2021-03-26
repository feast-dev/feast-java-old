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

import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import java.util.HashMap;

public class ProtoFeature implements Feature {
  private final ServingAPIProto.FeatureReferenceV2 featureReference;

  private final Timestamp eventTimestamp;

  private final ValueProto.Value featureValue;

  private static final HashMap<ValueProto.ValueType.Enum, ValueProto.Value.ValCase>
      TYPE_TO_VAL_CASE =
          new HashMap() {
            {
              put(ValueProto.ValueType.Enum.BYTES, ValueProto.Value.ValCase.BYTES_VAL);
              put(ValueProto.ValueType.Enum.STRING, ValueProto.Value.ValCase.STRING_VAL);
              put(ValueProto.ValueType.Enum.INT32, ValueProto.Value.ValCase.INT32_VAL);
              put(ValueProto.ValueType.Enum.INT64, ValueProto.Value.ValCase.INT64_VAL);
              put(ValueProto.ValueType.Enum.DOUBLE, ValueProto.Value.ValCase.DOUBLE_VAL);
              put(ValueProto.ValueType.Enum.FLOAT, ValueProto.Value.ValCase.FLOAT_VAL);
              put(ValueProto.ValueType.Enum.BOOL, ValueProto.Value.ValCase.BOOL_VAL);
              put(ValueProto.ValueType.Enum.BYTES_LIST, ValueProto.Value.ValCase.BYTES_LIST_VAL);
              put(ValueProto.ValueType.Enum.STRING_LIST, ValueProto.Value.ValCase.STRING_LIST_VAL);
              put(ValueProto.ValueType.Enum.INT32_LIST, ValueProto.Value.ValCase.INT32_LIST_VAL);
              put(ValueProto.ValueType.Enum.INT64_LIST, ValueProto.Value.ValCase.INT64_LIST_VAL);
              put(ValueProto.ValueType.Enum.DOUBLE_LIST, ValueProto.Value.ValCase.DOUBLE_LIST_VAL);
              put(ValueProto.ValueType.Enum.FLOAT_LIST, ValueProto.Value.ValCase.FLOAT_LIST_VAL);
              put(ValueProto.ValueType.Enum.BOOL_LIST, ValueProto.Value.ValCase.BOOL_LIST_VAL);
            }
          };

  public ProtoFeature(
      ServingAPIProto.FeatureReferenceV2 featureReference,
      Timestamp eventTimestamp,
      ValueProto.Value featureValue) {
    this.featureReference = featureReference;
    this.eventTimestamp = eventTimestamp;
    this.featureValue = featureValue;
  }

  @Override
  public ValueProto.Value getFeatureValue(ValueProto.ValueType.Enum valueType) {
    if (TYPE_TO_VAL_CASE.get(valueType) != this.featureValue.getValCase()) {
      return null;
    }

    return this.featureValue;
  }

  @Override
  public Boolean isSameFeatureSpec(ValueProto.ValueType.Enum valueType) {
    // Same feature reference, but different type
    if (valueType.equals(ValueProto.ValueType.Enum.INVALID)) {
      return false;
    }

    // Same feature reference, but empty value
    if (this.featureValue.getValCase().equals(ValueProto.Value.ValCase.VAL_NOT_SET)) {
      return true;
    }

    return TYPE_TO_VAL_CASE.get(valueType).equals(this.featureValue.getValCase());
  }

  @Override
  public ServingAPIProto.FeatureReferenceV2 getFeatureReference() {
    return this.featureReference;
  }

  @Override
  public Timestamp getEventTimestamp() {
    return this.eventTimestamp;
  }
}
