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
package feast.serving.registry;

import feast.proto.core.FeatureProto;
import feast.proto.core.FeatureViewProto;
import feast.proto.core.OnDemandFeatureViewProto;
import feast.proto.core.RegistryProto;
import feast.proto.serving.ServingAPIProto;
import feast.serving.exception.SpecRetrievalException;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalRegistryRepo implements RegistryRepository {
  private final RegistryProto.Registry registry;

  public LocalRegistryRepo(Path localRegistryPath) {
    if (!localRegistryPath.toFile().exists()) {
      throw new RuntimeException(
          String.format("Local registry not found at path %s", localRegistryPath));
    }
    try {
      final byte[] registryContents = Files.readAllBytes(localRegistryPath);
      this.registry = RegistryProto.Registry.parseFrom(registryContents);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RegistryProto.Registry getRegistry() {
    return this.registry;
  }

  @Override
  public FeatureViewProto.FeatureViewSpec getFeatureViewSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    final RegistryProto.Registry registry = this.getRegistry();
    for (final FeatureViewProto.FeatureView featureView : registry.getFeatureViewsList()) {
      if (featureView.getSpec().getName().equals(featureReference.getFeatureTable())) {
        return featureView.getSpec();
      }
    }
    throw new SpecRetrievalException(
        String.format(
            "Unable to find feature view with name: %s", featureReference.getFeatureTable()));
  }

  @Override
  public FeatureProto.FeatureSpecV2 getFeatureSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    final FeatureViewProto.FeatureViewSpec spec =
        this.getFeatureViewSpec(projectName, featureReference);
    for (final FeatureProto.FeatureSpecV2 featureSpec : spec.getFeaturesList()) {
      if (featureSpec.getName().equals(featureReference.getName())) {
        return featureSpec;
      }
    }

    throw new SpecRetrievalException(
        String.format(
            "Unable to find feature with name: %s in feature view: %s",
            featureReference.getName(), featureReference.getFeatureTable()));
  }

  @Override
  public OnDemandFeatureViewProto.OnDemandFeatureViewSpec getOnDemandFeatureViewSpec(
      String projectName, ServingAPIProto.FeatureReferenceV2 featureReference) {
    final RegistryProto.Registry registry = this.getRegistry();
    for (final OnDemandFeatureViewProto.OnDemandFeatureView onDemandFeatureView :
        registry.getOnDemandFeatureViewsList()) {
      if (onDemandFeatureView.getSpec().getName().equals(featureReference.getFeatureTable())) {
        return onDemandFeatureView.getSpec();
      }
    }
    throw new SpecRetrievalException(
        String.format(
            "Unable to find on demand feature view with name: %s",
            featureReference.getFeatureTable()));
  }

  @Override
  public boolean isBatchFeatureReference(ServingAPIProto.FeatureReferenceV2 featureReference) {
    final RegistryProto.Registry registry = this.getRegistry();
    for (final FeatureViewProto.FeatureView featureView : registry.getFeatureViewsList()) {
      if (featureView.getSpec().getName().equals(featureReference.getFeatureTable())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isOnDemandFeatureReference(ServingAPIProto.FeatureReferenceV2 featureReference) {
    final RegistryProto.Registry registry = this.getRegistry();
    for (final OnDemandFeatureViewProto.OnDemandFeatureView onDemandFeatureView :
        registry.getOnDemandFeatureViewsList()) {
      if (onDemandFeatureView.getSpec().getName().equals(featureReference.getFeatureTable())) {
        return true;
      }
    }
    return false;
  }
}
