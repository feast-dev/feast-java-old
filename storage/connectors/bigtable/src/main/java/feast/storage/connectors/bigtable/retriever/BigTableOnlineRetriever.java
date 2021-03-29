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
package feast.storage.connectors.bigtable.retriever;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.NativeFeature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

public class BigTableOnlineRetriever implements OnlineRetrieverV2 {

  private BigtableDataClient client;
  private BigTableSchemaRegistry schemaRegistry;

  public BigTableOnlineRetriever(BigtableDataClient client) {
    this.client = client;
    this.schemaRegistry = new BigTableSchemaRegistry(client);
  }

  private String getTableName(String project, List<String> entityNames) {
    String tableName =
        String.format("%s__%s", project, entityNames.stream().collect(Collectors.joining("__")));

    return tableName;
  }

  private String valueToString(ValueProto.Value v) {
    String stringRepr;
    switch (v.getValCase()) {
      case STRING_VAL:
        stringRepr = v.getStringVal();
        break;
      case INT64_VAL:
        stringRepr = String.valueOf(v.getInt64Val());
        break;
      case INT32_VAL:
        stringRepr = String.valueOf(v.getInt32Val());
        break;
      case BYTES_VAL:
        stringRepr = v.getBytesVal().toString();
        break;
      default:
        throw new RuntimeException("Type is not supported to be entity");
    }

    return stringRepr;
  }

  private ByteString convertEntityValueToBigTableKey(
      EntityRow entityRow, List<String> entityNames) {
    return ByteString.copyFrom(
        entityNames.stream()
            .map(entity -> entityRow.getFieldsMap().get(entity))
            .map(this::valueToString)
            .collect(Collectors.joining("#"))
            .getBytes());
  }

  private List<String> getColumnFamilies(List<FeatureReferenceV2> featureReferences) {
    return featureReferences.stream()
        .map(FeatureReferenceV2::getFeatureTable)
        .collect(Collectors.toList());
  }

  private List<Feature> decodeFeatures(
      String tableName,
      ByteString value,
      List<FeatureReferenceV2> featureReferences,
      long timestamp)
      throws IOException {
    ByteString schemaReferenceBytes = value.substring(0, 4);
    byte[] featureValueBytes = value.substring(4).toByteArray();

    BigTableSchemaRegistry.SchemaReference schemaReference =
        new BigTableSchemaRegistry.SchemaReference(tableName, schemaReferenceBytes);

    Schema schema = schemaRegistry.getSchema(schemaReference);

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(featureValueBytes, null);
    GenericRecord record = reader.read(null, decoder);

    return featureReferences.stream()
        .map(
            featureReference -> {
              Object featureValue;
              try {
                featureValue = record.get(featureReference.getName());
              } catch (AvroRuntimeException e) {
                return null;
              }
              if (featureValue != null) {
                return new NativeFeature(
                    featureReference,
                    Timestamp.newBuilder().setSeconds(timestamp / 1000).build(),
                    featureValue);
              }
              return null;
            })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public List<List<Feature>> getOnlineFeatures(
      String project,
      List<EntityRow> entityRows,
      List<FeatureReferenceV2> featureReferences,
      List<String> entityNames) {
    List<String> columnFamilies = getColumnFamilies(featureReferences);
    String tableName = getTableName(project, entityNames);

    ServerStream<Row> rowsFromBigTable =
        getFeaturesFromBigTable(tableName, entityRows, columnFamilies, entityNames);
    List<List<Feature>> features =
        convertRowToFeature(tableName, rowsFromBigTable, featureReferences);

    return features;
  }

  private ServerStream<Row> getFeaturesFromBigTable(
      String tableName,
      List<EntityRow> entityRows,
      List<String> columnFamilies,
      List<String> entityNames) {

    Query rowQuery = Query.create(tableName);
    Filters.InterleaveFilter familyFilter = Filters.FILTERS.interleave();
    columnFamilies.forEach(cf -> familyFilter.filter(Filters.FILTERS.family().exactMatch(cf)));

    for (EntityRow row : entityRows) {
      ByteString rowKey = convertEntityValueToBigTableKey(row, entityNames);
      rowQuery = rowQuery.rowKey(rowKey);
    }

    return client.readRows(rowQuery);
  }

  private List<List<Feature>> convertRowToFeature(
      String tableName, ServerStream<Row> rows, List<FeatureReferenceV2> featureReferences) {

    return StreamSupport.stream(rows.spliterator(), false)
        .flatMap(
            row ->
                row.getCells().stream()
                    .map(
                        rowCell -> {
                          String family = rowCell.getFamily();
                          ByteString value = rowCell.getValue();

                          List<Feature> features;
                          List<FeatureReferenceV2> localFeatureReferences =
                              featureReferences.stream()
                                  .filter(
                                      featureReference ->
                                          featureReference.getFeatureTable().equals(family))
                                  .collect(Collectors.toList());

                          try {
                            features =
                                decodeFeatures(
                                    tableName,
                                    value,
                                    localFeatureReferences,
                                    rowCell.getTimestamp());
                          } catch (IOException e) {
                            throw new RuntimeException("Failed to decode features from BigTable");
                          }

                          return features;
                        }))
        .collect(Collectors.toList());
  }
}
