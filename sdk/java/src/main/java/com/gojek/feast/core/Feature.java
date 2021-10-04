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

import feast.proto.core.FeatureProto;
import java.util.HashMap;
import java.util.Map;

public class Feature {
  private final String name;
  private final ValueType valueType;
  private final Map<String, String> labels;

  public static Builder getBuilder(String name, ValueType valueType) {
    return new Builder(name, valueType);
  }

  protected Feature(String name, ValueType valueType, Map<String, String> labels) {
    this.name = name;
    this.valueType = valueType;
    this.labels = labels;
  }

  public static class Builder {
    private final String name;
    private final ValueType valueType;
    private final Map<String, String> labels = new HashMap<>();

    private Builder(String name, ValueType valueType) {
      this.name = name;
      this.valueType = valueType;
    }

    public Builder addLabel(String key, String val) {
      this.labels.put(key, val);
      return this;
    }

    public Builder addLabels(Map<String, String> labels) {
      this.labels.putAll(labels);
      return this;
    }

    public Feature build() {
      return new Feature(name, valueType, labels);
    }
  }

  public String getName() {
    return name;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  protected Feature(FeatureProto.FeatureSpecV2 feature) {
    this.name = feature.getName();
    this.valueType = ValueType.fromProto(feature.getValueType());
    this.labels = feature.getLabelsMap();
  }

  protected FeatureProto.FeatureSpecV2 toProto() {
    return FeatureProto.FeatureSpecV2.newBuilder()
        .setName(name)
        .setValueType(valueType.toProto())
        .putAllLabels(labels)
        .build();
  }
}
