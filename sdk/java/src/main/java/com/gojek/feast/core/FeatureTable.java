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

import feast.proto.core.FeatureTableProto;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class FeatureTable {
  private Spec spec;
  private Metadata metadata;

  public static class Spec {
    private final String project;
    private final String name;
    private final List<String> entities;
    private final List<Feature> features;
    private final Map<String, String> labels;
    private Duration maxAge;

    protected Spec(
        String project,
        String name,
        List<String> entities,
        List<Feature> features,
        Map<String, String> labels,
        Duration maxAge) {
      this.project = project;
      this.name = name;
      this.entities = entities;
      this.features = features;
      this.labels = labels;
      this.maxAge = maxAge;
    }

    protected Spec(
        String project,
        String name,
        List<String> entities,
        List<Feature> features,
        Map<String, String> labels) {
      this(project, name, entities, features, labels, null);
    }

    public static Builder getBuilder(String project, String name) {
      return new Builder(project, name);
    }

    public static class Builder {
      private final String project;
      private final String name;
      private final List<String> entities = new LinkedList<>();
      private final List<Feature> features = new LinkedList<>();
      private final Map<String, String> labels = new HashMap<>();
      private Duration maxAge;

      private Builder(String project, String name) {
        this.project = project;
        this.name = name;
      }

      public Spec build() {
        return new Spec(project, name, entities, features, labels, maxAge);
      }

      public Builder addEntity(String entity) {
        this.entities.add(entity);
        return this;
      }

      public Builder addEntities(Collection<String> entities) {
        this.entities.addAll(entities);
        return this;
      }

      public Builder addFeature(Feature feature) {
        this.features.add(feature);
        return this;
      }

      public Builder addFeatures(Collection<? extends Feature> features) {
        this.features.addAll(features);
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

      public Builder setMaxAge(Duration maxAge) {
        this.maxAge = maxAge;
        return this;
      }
    }

    public String getProject() {
      return project;
    }

    public String getName() {
      return name;
    }

    public List<String> getEntities() {
      return entities;
    }

    public List<Feature> getFeatures() {
      return features;
    }

    public Map<String, String> getLabels() {
      return labels;
    }

    public Optional<Duration> getMaxAge() {
      return Optional.ofNullable(maxAge);
    }

    protected Spec(FeatureTableProto.FeatureTableSpec spec) {
      this.project = spec.getProject();
      this.name = spec.getName();
      this.entities = spec.getEntitiesList();
      this.features =
          spec.getFeaturesList().stream().map(Feature::new).collect(Collectors.toList());
      this.labels = spec.getLabelsMap();
      if (spec.hasMaxAge())
        this.maxAge =
            Duration.ofSeconds(spec.getMaxAge().getSeconds(), spec.getMaxAge().getNanos());
    }

    protected FeatureTableProto.FeatureTableSpec toProto() {
      FeatureTableProto.FeatureTableSpec.Builder builder =
          FeatureTableProto.FeatureTableSpec.newBuilder()
              .setProject(project)
              .setName(name)
              .addAllEntities(entities)
              .addAllFeatures(features.stream().map(Feature::toProto).collect(Collectors.toList()))
              .putAllLabels(labels);
      if (maxAge != null) {
        builder.setMaxAge(
            com.google.protobuf.Duration.newBuilder()
                .setSeconds(maxAge.getSeconds())
                .setNanos(maxAge.getNano())
                .build());
      }
      return builder.build();
    }
  }

  public Spec getSpec() {
    return spec;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  protected FeatureTable(FeatureTableProto.FeatureTable featureTable) {
    if (featureTable.hasSpec()) this.spec = new Spec(featureTable.getSpec());
    if (featureTable.hasMeta()) this.metadata = new Metadata(featureTable.getMeta());
  }

  protected FeatureTableProto.FeatureTable toProto() {
    return FeatureTableProto.FeatureTable.newBuilder()
        .setSpec(spec.toProto())
        .setMeta(
            FeatureTableProto.FeatureTableMeta.newBuilder()
                .setLastUpdatedTimestamp(Metadata.toProto(metadata.getLastUpdatedTimestamp()))
                .setCreatedTimestamp(Metadata.toProto(metadata.getCreatedTimestamp())))
        .build();
  }
}
