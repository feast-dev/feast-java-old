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

import feast.proto.core.EntityProto;
import java.util.HashMap;
import java.util.Map;

public class Entity {
  private Spec spec;
  private Metadata metadata;

  public static class Spec {
    private final String name;
    private final String project;
    private final ValueType value;
    private final String description;
    private final Map<String, String> labels;

    protected Spec(
        String project,
        String name,
        ValueType value,
        String description,
        Map<String, String> labels) {
      this.name = name;
      this.project = project;
      this.value = value;
      this.description = description;
      this.labels = labels;
    }

    public static Builder getBuilder(String name, String project) {
      return new Builder(name, project);
    }

    public static class Builder {
      private final String name;
      private final String project;
      private ValueType value;
      private String description;
      private final Map<String, String> labels = new HashMap<>();

      private Builder(String project, String name) {
        this.project = project;
        this.name = name;
      }

      public Builder setValue(ValueType value) {
        this.value = value;
        return this;
      }

      public Builder setDescription(String description) {
        this.description = description;
        return this;
      }

      public Builder addLabel(String key, String val) {
        this.labels.put(key, val);
        return this;
      }

      public Builder addLabels(Map<String, String> labels) {
        this.labels.putAll(labels);
        return this;
      }

      public Spec build() {
        return new Spec(project, name, value, description, labels);
      }
    }

    public String getName() {
      return name;
    }

    public String getProject() {
      return project;
    }

    public ValueType getValue() {
      return value;
    }

    public String getDescription() {
      return description;
    }

    public Map<String, String> getLabels() {
      return labels;
    }

    protected Spec(String project, EntityProto.EntitySpecV2 spec) {
      this.project = project;
      this.name = spec.getName();
      this.value = ValueType.fromProto(spec.getValueType());
      this.description = spec.getDescription();
      this.labels = spec.getLabelsMap();
    }

    protected EntityProto.EntitySpecV2 toProto() {
      return EntityProto.EntitySpecV2.newBuilder()
          .setName(name)
          .setValueType(value.toProto())
          .setDescription(description)
          .putAllLabels(labels)
          .build();
    }
  }

  public Spec getSpec() {
    return spec;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  protected Entity(String project, EntityProto.Entity entity) {
    if (entity.hasSpec()) this.spec = new Spec(project, entity.getSpec());
    if (entity.hasMeta()) this.metadata = new Metadata(entity.getMeta());
  }

  protected EntityProto.Entity toProto() {
    return EntityProto.Entity.newBuilder()
        .setSpec(spec.toProto())
        .setMeta(
            EntityProto.EntityMeta.newBuilder()
                .setCreatedTimestamp(Metadata.toProto(metadata.getCreatedTimestamp()))
                .setLastUpdatedTimestamp(Metadata.toProto(metadata.getLastUpdatedTimestamp())))
        .build();
  }
}
