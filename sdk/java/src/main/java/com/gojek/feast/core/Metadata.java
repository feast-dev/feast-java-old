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
import feast.proto.core.EntityProto;
import feast.proto.core.FeatureTableProto;
import java.time.Instant;

public class Metadata {
  private final Instant createdTimestamp;
  private final Instant lastUpdatedTimestamp;

  protected Metadata(EntityProto.EntityMeta meta) {
    this.createdTimestamp = toInstant(meta.getCreatedTimestamp());
    this.lastUpdatedTimestamp = toInstant(meta.getLastUpdatedTimestamp());
  }

  protected Metadata(FeatureTableProto.FeatureTableMeta meta) {
    this.createdTimestamp = toInstant(meta.getCreatedTimestamp());
    this.lastUpdatedTimestamp = toInstant(meta.getLastUpdatedTimestamp());
  }

  public Instant getCreatedTimestamp() {
    return createdTimestamp;
  }

  public Instant getLastUpdatedTimestamp() {
    return lastUpdatedTimestamp;
  }

  protected static Instant toInstant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  protected static Timestamp toProto(Instant instant) {
    return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).build();
  }
}
