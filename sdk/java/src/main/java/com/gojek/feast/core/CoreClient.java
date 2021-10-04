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

import com.gojek.feast.GrpcManager;
import com.gojek.feast.SecurityConfig;
import feast.proto.core.*;
import feast.proto.core.CoreServiceGrpc.CoreServiceBlockingStub;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class CoreClient extends GrpcManager<CoreServiceBlockingStub> {

  /**
   * Create a client to access Feast Core.
   *
   * @param host hostname or ip address of Feast core GRPC server
   * @param port port number of Feast core GRPC server
   * @return {@link CoreClient}
   */
  public static CoreClient create(String host, int port) {
    // configure client with no security config.
    return CoreClient.createSecure(host, port, SecurityConfig.newBuilder().build());
  }

  /**
   * Create an authenticated client that can access Feast core with authentication enabled. Supports
   * the {@link CallCredentials} in the {@link feast.common.auth.credentials} package.
   *
   * @param host hostname or ip address of Feast core GRPC server
   * @param port port number of Feast core GRPC server
   * @param securityConfig security options to configure the Feast client. See {@link
   *     SecurityConfig} for options.
   * @return Optional of {@link CoreClient}
   */
  public static CoreClient createSecure(String host, int port, SecurityConfig securityConfig) {
    return new CoreClient(host, port, securityConfig);
  }

  /**
   * Create a globally unique namespace to store Feature Tables in.
   *
   * @param name of the project.
   * @return true if request returned, false otherwise.
   */
  public boolean createProject(String name) {
    CoreServiceProto.CreateProjectRequest request =
        CoreServiceProto.CreateProjectRequest.newBuilder().setName(name).build();
    CoreServiceProto.CreateProjectResponse response = stub.createProject(request);
    return response != null;
  }

  /**
   * Archives a project. Archived projects will continue to exist and function, but won't be visible
   * through the Core API. Any existing ingestion or serving requests will continue to function, but
   * will result in warning messages being logged. It is not possible to unarchive a project through
   * the Core API.
   *
   * @param name of the project to archive.
   * @return true if request returned, false otherwise.
   */
  public boolean archiveProject(String name) {
    CoreServiceProto.ArchiveProjectRequest request =
        CoreServiceProto.ArchiveProjectRequest.newBuilder().setName(name).build();
    CoreServiceProto.ArchiveProjectResponse response = stub.archiveProject(request);
    return response != null;
  }

  /**
   * List all active projects.
   *
   * @return the list of project names.
   */
  public List<String> listProjects() {
    return stub.listProjects(CoreServiceProto.ListProjectsRequest.newBuilder().build())
        .getProjectsList();
  }

  /**
   * Returns a specific entity.
   *
   * @param project name
   * @param name of the entity
   * @return Optional of {@link Entity}
   */
  public Optional<Entity> getEntity(String project, String name) {
    CoreServiceProto.GetEntityRequest.Builder request =
        CoreServiceProto.GetEntityRequest.newBuilder().setProject(project).setName(name);
    CoreServiceProto.GetEntityResponse response = stub.getEntity(request.build());
    Entity entity = response.hasEntity() ? new Entity(response.getEntity()) : null;
    return Optional.ofNullable(entity);
  }

  /**
   * Create or update and existing entity. Schema changes will update the entity if the changes are
   * valid. Can't update name or type.
   *
   * @param entitySpec {@link Entity.Spec}
   * @return Optional of {@link Entity}
   */
  public Optional<Entity> apply(Entity.Spec entitySpec) {
    CoreServiceProto.ApplyEntityRequest.Builder request =
        CoreServiceProto.ApplyEntityRequest.newBuilder()
            .setProject(entitySpec.getProject())
            .setSpec(entitySpec.toProto());
    CoreServiceProto.ApplyEntityResponse response = stub.applyEntity(request.build());
    Entity entity = response.hasEntity() ? new Entity(response.getEntity()) : null;
    return Optional.ofNullable(entity);
  }

  /**
   * List all entities under a certain project.
   *
   * @param project specifies the name of the project to list Entities in.
   * @return all entity references under projectt.
   */
  public List<Entity> listEntities(String project) {
    return listEntities(project, null);
  }

  /**
   * List all entities under a certain project.
   *
   * @param project specifies the name of the project to list Entities in.
   * @param labels are User defined metadata for entity. Entities with all matching labels will be
   *     returned.
   * @return all entity references and respective entities matching labels under project.
   */
  public List<Entity> listEntities(String project, Map<String, String> labels) {
    CoreServiceProto.ListEntitiesRequest.Filter.Builder filter =
        CoreServiceProto.ListEntitiesRequest.Filter.newBuilder().setProject(project);
    if (labels != null) filter.putAllLabels(labels);
    CoreServiceProto.ListEntitiesRequest request =
        CoreServiceProto.ListEntitiesRequest.newBuilder().setFilter(filter).build();
    List<EntityProto.Entity> entities = stub.listEntities(request).getEntitiesList();
    return entities.stream().map(Entity::new).collect(Collectors.toList());
  }

  /**
   * Returns a specific feature table.
   *
   * @param project name
   * @param name of the feature table
   * @return Optional of {@link FeatureTable}
   */
  public Optional<FeatureTable> getFeatureTable(String project, String name) {
    CoreServiceProto.GetFeatureTableRequest.Builder request =
        CoreServiceProto.GetFeatureTableRequest.newBuilder().setProject(project).setName(name);
    CoreServiceProto.GetFeatureTableResponse response = stub.getFeatureTable(request.build());
    FeatureTable featureTable = response.hasTable() ? new FeatureTable(response.getTable()) : null;
    return Optional.ofNullable(featureTable);
  }

  /**
   * Create or update an existing feature table. Schema changes will update the feature table if the
   * changes are valid. Can't update name, entities and feature names and types.
   *
   * @param featureTableSpec {@link FeatureTable.Spec}
   * @return Optional of {@link FeatureTable}
   */
  public Optional<FeatureTable> apply(FeatureTable.Spec featureTableSpec) {
    CoreServiceProto.ApplyFeatureTableRequest.Builder request =
        CoreServiceProto.ApplyFeatureTableRequest.newBuilder()
            .setProject(featureTableSpec.getProject())
            .setTableSpec(featureTableSpec.toProto());
    CoreServiceProto.ApplyFeatureTableResponse response = stub.applyFeatureTable(request.build());
    FeatureTable featureTable = response.hasTable() ? new FeatureTable(response.getTable()) : null;
    return Optional.ofNullable(featureTable);
  }

  /**
   * List all Feature Tables under a project.
   *
   * @param project name.
   * @return a list of the matching {@link FeatureTable}.
   */
  public List<FeatureTable> listFeatureTables(String project) {
    return listFeatureTables(project, null);
  }

  /**
   * List all Feature Tables under a project.
   *
   * @param project name.
   * @param labels on Feature Tables will be returned.
   * @return a list of the matching {@link FeatureTable}.
   */
  public List<FeatureTable> listFeatureTables(String project, Map<String, String> labels) {
    CoreServiceProto.ListFeatureTablesRequest.Filter.Builder filter =
        CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder().setProject(project);
    if (labels != null) filter.putAllLabels(labels);
    CoreServiceProto.ListFeatureTablesRequest request =
        CoreServiceProto.ListFeatureTablesRequest.newBuilder().setFilter(filter).build();
    List<FeatureTableProto.FeatureTable> featureTables =
        stub.listFeatureTables(request).getTablesList();
    return featureTables.stream().map(FeatureTable::new).collect(Collectors.toList());
  }

  /**
   * @param project name that the feature tables belongs to
   * @param entities contained within the featureSet that the feature belongs to. Only feature
   *     tables with these entities will be searched for features. If none are specified all Feature
   *     Tables will be seeked.
   * @return list of Features matching the parameters.
   */
  public List<Feature> listFeatures(String project, String... entities) {
    return listFeatures(project, Arrays.asList(entities), null);
  }

  /**
   * @param project name that the feature tables belongs to
   * @param entities contained within the featureSet that the feature belongs to. Only feature
   *     tables with these entities will be searched for features.
   * @param labels are user defined metadata for feature. Features with all matching labels will be
   *     returned.
   * @return list of Features matching the parameters.
   */
  public List<Feature> listFeatures(
      String project, List<String> entities, Map<String, String> labels) {
    CoreServiceProto.ListFeaturesRequest.Filter.Builder filter =
        CoreServiceProto.ListFeaturesRequest.Filter.newBuilder().setProject(project);
    if (entities != null) filter.addAllEntities(entities);
    if (labels != null) filter.putAllLabels(labels);
    CoreServiceProto.ListFeaturesRequest request =
        CoreServiceProto.ListFeaturesRequest.newBuilder().setFilter(filter).build();
    Map<String, FeatureProto.FeatureSpecV2> features = stub.listFeatures(request).getFeaturesMap();
    return features.values().stream().map(Feature::new).collect(Collectors.toList());
  }

  /**
   * Delete a specific Feature Table.
   *
   * @param project Name of the Project to delete the Feature Table from.
   * @param name of the FeatureTable to delete.
   * @return true if request returned, false otherwise.
   */
  public boolean deleteFeatureTable(String project, String name) {
    CoreServiceProto.DeleteFeatureTableRequest request =
        CoreServiceProto.DeleteFeatureTableRequest.newBuilder()
            .setProject(project)
            .setName(name)
            .build();
    return stub.deleteFeatureTable(request) != null;
  }

  protected CoreClient(String host, int port, SecurityConfig securityConfig) {
    super(host, port, securityConfig);
  }

  protected CoreClient(ManagedChannel channel, Optional<CallCredentials> credentials) {
    super(channel, credentials);
  }

  @Override
  protected CoreServiceBlockingStub getStub(Channel channel) {
    return CoreServiceGrpc.newBlockingStub(channel);
  }
}
