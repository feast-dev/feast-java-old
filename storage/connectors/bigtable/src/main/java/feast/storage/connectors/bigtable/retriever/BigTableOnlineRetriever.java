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
import java.util.*;
import java.util.function.Function;
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

  /**
   * Generate name of BigTable table in the form of <feastProject>__<entityNames>
   *
   * @param project Name of Feast project
   * @param entityNames List of entities used in retrieval call
   * @return Name of BigTable table
   */
  private String getTableName(String project, List<String> entityNames) {
    String tableName =
        String.format("%s__%s", project, entityNames.stream().collect(Collectors.joining("__")));

    return tableName;
  }

  /**
   * Convert Entity value from Feast valueType to String type. Currently only supports STRING_VAL,
   * INT64_VAL, INT32_VAL and BYTES_VAL.
   *
   * @param v Entity value of Feast valueType
   * @return String representation of Entity value
   */
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

  /**
   * Generate BigTable key in the form of entity values joined by #.
   *
   * @param entityRow Single EntityRow representation in feature retrieval call
   * @param entityNames List of entities related to feature references in retrieval call
   * @return BigTable key for retrieval
   */
  private ByteString convertEntityValueToBigTableKey(
      EntityRow entityRow, List<String> entityNames) {
    return ByteString.copyFrom(
        entityNames.stream()
            .map(entity -> entityRow.getFieldsMap().get(entity))
            .map(this::valueToString)
            .collect(Collectors.joining("#"))
            .getBytes());
  }

  /**
   * Retrieve BigTable table column families based on FeatureTable names.
   *
   * @param featureReferences List of feature references of features in retrieval call
   * @return List of String of FeatureTable names
   */
  private List<String> getColumnFamilies(List<FeatureReferenceV2> featureReferences) {
    return featureReferences.stream()
        .map(FeatureReferenceV2::getFeatureTable)
        .collect(Collectors.toList());
  }

  /**
   * AvroRuntimeException is thrown if feature name does not exist in avro schema. Empty Object is
   * returned when null is retrieved from BigTable RowCell.
   *
   * @param tableName Name of BigTable table
   * @param value Value of BigTable cell where first 4 bytes represent the schema reference and
   *     remaining bytes represent avro-serialized features
   * @param featureReferences List of feature references
   * @param timestamp Timestamp of rowCell
   * @return @NativeFeature with retrieved value stored in BigTable RowCell
   * @throws IOException
   */
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
                // Feature is not found in schema
                return null;
              }
              if (featureValue != null) {
                return new NativeFeature(
                    featureReference,
                    Timestamp.newBuilder().setSeconds(timestamp / 1000).build(),
                    featureValue);
              }
              return new NativeFeature(
                  featureReference,
                  Timestamp.newBuilder().setSeconds(timestamp / 1000).build(),
                  new Object());
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

    List<ByteString> rowKeys =
        entityRows.stream()
            .map(row -> convertEntityValueToBigTableKey(row, entityNames))
            .collect(Collectors.toList());
    Map<ByteString, Row> rowsFromBigTable =
        getFeaturesFromBigTable(tableName, rowKeys, columnFamilies);
    List<List<Feature>> features =
        convertRowToFeature(tableName, rowKeys, rowsFromBigTable, featureReferences);

    return features;
  }

  /**
   * Retrieve rows for each row entity key by generating BigTable rowQuery with filters based on
   * column families.
   *
   * @param tableName Name of BigTable table
   * @param rowKeys List of keys of rows to retrieve
   * @param columnFamilies List of FeatureTable names
   * @return Map of retrieved features for each rowKey
   */
  private Map<ByteString, Row> getFeaturesFromBigTable(
      String tableName, List<ByteString> rowKeys, List<String> columnFamilies) {

    Query rowQuery = Query.create(tableName);
    Filters.InterleaveFilter familyFilter = Filters.FILTERS.interleave();
    columnFamilies.forEach(cf -> familyFilter.filter(Filters.FILTERS.family().exactMatch(cf)));

    for (ByteString rowKey : rowKeys) {
      rowQuery.rowKey(rowKey);
    }

    return StreamSupport.stream(client.readRows(rowQuery).spliterator(), false)
        .collect(Collectors.toMap(Row::getKey, Function.identity()));
  }

  /**
   * Converts rowCell feature value into @NativeFeature type.
   *
   * @param tableName Name of BigTable table
   * @param rowKeys List of keys of rows to retrieve
   * @param rows Map of rowKey to Row related to it
   * @param featureReferences List of feature references
   * @return List of List of Features associated with respective rowKey
   */
  private List<List<Feature>> convertRowToFeature(
      String tableName,
      List<ByteString> rowKeys,
      Map<ByteString, Row> rows,
      List<FeatureReferenceV2> featureReferences) {

    return rowKeys.stream()
        .map(
            rowKey -> {
              if (!rows.containsKey(rowKey)) {
                return Collections.<Feature>emptyList();
              } else {
                return rows.get(rowKey).getCells().stream()
                    .flatMap(
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

                          return features.stream();
                        })
                    .collect(Collectors.toList());
              }
            })
        .collect(Collectors.toList());
  }
}
