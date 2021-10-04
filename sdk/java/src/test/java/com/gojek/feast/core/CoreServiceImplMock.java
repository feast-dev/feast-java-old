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

import com.google.protobuf.Timestamp;
import feast.proto.core.CoreServiceGrpc;
import feast.proto.core.CoreServiceProto;
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureTableProto;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CoreServiceImplMock extends CoreServiceGrpc.CoreServiceImplBase {
  protected static final String PROJECT = "project";

  private static final Map<String, EntityProto.Entity.Builder> entities = new ConcurrentHashMap<>();
  private static final Map<String, FeatureTableProto.FeatureTable.Builder> featureTables =
      new ConcurrentHashMap<>();

  public static void clear() {
    entities.clear();
    featureTables.clear();
  }

  @Override
  public void getEntity(
      CoreServiceProto.GetEntityRequest request,
      StreamObserver<CoreServiceProto.GetEntityResponse> responseObserver) {
    if (!request.getProject().equals(PROJECT))
      responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    CoreServiceProto.GetEntityResponse.Builder response =
        CoreServiceProto.GetEntityResponse.newBuilder();
    entities.computeIfPresent(
        request.getName(),
        (k, v) -> {
          response.setEntity(v);
          return v;
        });
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void applyEntity(
      CoreServiceProto.ApplyEntityRequest request,
      StreamObserver<CoreServiceProto.ApplyEntityResponse> responseObserver) {
    if (!request.getProject().equals(PROJECT))
      responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    EntityProto.EntitySpecV2 spec = request.getSpec();
    EntityProto.Entity.Builder entity =
        entities.computeIfAbsent(
            request.getSpec().getName(),
            k ->
                EntityProto.Entity.newBuilder()
                    .setSpec(spec)
                    .setMeta(EntityProto.EntityMeta.newBuilder().setCreatedTimestamp(now())));
    EntityProto.EntitySpecV2 oldSpec = entity.getSpec();
    if (!oldSpec.getName().equals(spec.getName())
        || !oldSpec.getValueType().equals(spec.getValueType()))
      responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
    entity.setSpec(spec).getMetaBuilder().setLastUpdatedTimestamp(now());
    responseObserver.onNext(
        CoreServiceProto.ApplyEntityResponse.newBuilder().setEntity(entity).build());
    responseObserver.onCompleted();
  }

  @Override
  public void applyFeatureTable(
      CoreServiceProto.ApplyFeatureTableRequest request,
      StreamObserver<CoreServiceProto.ApplyFeatureTableResponse> responseObserver) {
    if (!request.getProject().equals(PROJECT))
      responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    FeatureTableProto.FeatureTableSpec spec = request.getTableSpec();
    FeatureTableProto.FeatureTable.Builder featureTable =
        featureTables.computeIfAbsent(
            spec.getName(),
            k ->
                FeatureTableProto.FeatureTable.newBuilder()
                    .setSpec(spec)
                    .setMeta(
                        FeatureTableProto.FeatureTableMeta.newBuilder()
                            .setCreatedTimestamp(now())));
    FeatureTableProto.FeatureTableSpec oldSpec = featureTable.getSpec();
    if (!oldSpec.getName().equals(spec.getName())
        || !oldSpec.getEntitiesList().equals(spec.getEntitiesList()))
      responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
    featureTable.setSpec(spec).getMetaBuilder().setLastUpdatedTimestamp(now());
    responseObserver.onNext(
        CoreServiceProto.ApplyFeatureTableResponse.newBuilder().setTable(featureTable).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getFeatureTable(
      CoreServiceProto.GetFeatureTableRequest request,
      StreamObserver<CoreServiceProto.GetFeatureTableResponse> responseObserver) {
    if (!request.getProject().equals(PROJECT))
      responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    CoreServiceProto.GetFeatureTableResponse.Builder response =
        CoreServiceProto.GetFeatureTableResponse.newBuilder();
    featureTables.computeIfPresent(
        request.getName(),
        (k, v) -> {
          response.setTable(v);
          return v;
        });
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void listFeatures(
      CoreServiceProto.ListFeaturesRequest request,
      StreamObserver<CoreServiceProto.ListFeaturesResponse> responseObserver) {
    if (!request.getFilter().getProject().equals(PROJECT))
      responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    if (request.getFilter().getEntitiesCount() > 0 || request.getFilter().getLabelsCount() > 0)
      responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
    CoreServiceProto.ListFeaturesResponse.Builder response =
        CoreServiceProto.ListFeaturesResponse.newBuilder();
    featureTables.values().stream()
        .flatMap(featureTable -> featureTable.getSpec().getFeaturesList().stream())
        .forEach(feature -> response.putFeatures(feature.getName(), feature));
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void listEntities(
      CoreServiceProto.ListEntitiesRequest request,
      StreamObserver<CoreServiceProto.ListEntitiesResponse> responseObserver) {
    if (!request.getFilter().getProject().equals(PROJECT))
      responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    if (request.getFilter().getLabelsCount() > 0)
      responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
    CoreServiceProto.ListEntitiesResponse.Builder response =
        CoreServiceProto.ListEntitiesResponse.newBuilder();
    entities.values().forEach(response::addEntities);
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void createProject(
      CoreServiceProto.CreateProjectRequest request,
      StreamObserver<CoreServiceProto.CreateProjectResponse> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
  }

  @Override
  public void archiveProject(
      CoreServiceProto.ArchiveProjectRequest request,
      StreamObserver<CoreServiceProto.ArchiveProjectResponse> responseObserver) {
    responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
  }

  @Override
  public void listProjects(
      CoreServiceProto.ListProjectsRequest request,
      StreamObserver<CoreServiceProto.ListProjectsResponse> responseObserver) {
    responseObserver.onNext(
        CoreServiceProto.ListProjectsResponse.newBuilder().addProjects(PROJECT).build());
    responseObserver.onCompleted();
  }

  @Override
  public void listFeatureTables(
      CoreServiceProto.ListFeatureTablesRequest request,
      StreamObserver<CoreServiceProto.ListFeatureTablesResponse> responseObserver) {
    if (!request.getFilter().getProject().equals(PROJECT))
      responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    if (request.getFilter().getLabelsCount() > 0)
      responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
    CoreServiceProto.ListFeatureTablesResponse.Builder response =
        CoreServiceProto.ListFeatureTablesResponse.newBuilder();
    featureTables.values().forEach(response::addTables);
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  @Override
  public void deleteFeatureTable(
      CoreServiceProto.DeleteFeatureTableRequest request,
      StreamObserver<CoreServiceProto.DeleteFeatureTableResponse> responseObserver) {
    if (!request.getProject().equals(PROJECT))
      responseObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    featureTables.remove(request.getName());
    CoreServiceProto.DeleteFeatureTableResponse.Builder response =
        CoreServiceProto.DeleteFeatureTableResponse.newBuilder();
    responseObserver.onNext(response.build());
    responseObserver.onCompleted();
  }

  private Timestamp now() {
    return Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();
  }
}
