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
package feast.storage.connectors.cassandra.retriever;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto.FeatureReferenceV2;
import feast.proto.serving.ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.NativeFeature;
import feast.storage.connectors.sstable.retriever.SSTableOnlineRetriever;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

public class CassandraOnlineRetriever implements SSTableOnlineRetriever<ByteBuffer, Row> {

  private final CqlSession session;
  private final CassandraSchemaRegistry schemaRegistry;

  private static final String ENTITY_KEY = "key";
  private static final String SCHEMA_REF_SUFFIX = "__schema_ref";
  private static final String EVENT_TIMESTAMP_SUFFIX = "__event_timestamp";

  public CassandraOnlineRetriever(CqlSession session) {
    this.session = session;
    this.schemaRegistry = new CassandraSchemaRegistry(session);
  }

  /**
   * Generate Cassandra key in the form of entity values joined by #.
   *
   * @param entityRow Single EntityRow representation in feature retrieval call
   * @param entityNames List of entities related to feature references in retrieval call
   * @return Cassandra key for retrieval
   */
  @Override
  public ByteBuffer convertEntityValueToKey(EntityRow entityRow, List<String> entityNames) {
    return ByteBuffer.wrap(
        entityNames.stream()
            .map(entity -> entityRow.getFieldsMap().get(entity))
            .map(this::valueToString)
            .collect(Collectors.joining("#"))
            .getBytes());
  }

  /**
   * Converts Cassandra rows into @NativeFeature type.
   *
   * @param rowKeys List of keys of rows to retrieve
   * @param rows Map of rowKey to Row related to it
   * @param featureReferences List of feature references
   * @return List of List of Features associated with respective rowKey
   */
  @Override
  public List<List<Feature>> convertRowToFeature(
      String tableName,
      List<ByteBuffer> rowKeys,
      Map<ByteBuffer, Row> rows,
      List<FeatureReferenceV2> featureReferences) {

    BinaryDecoder reusedDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);

    return rowKeys.stream()
        .map(
            rowKey -> {
              if (!rows.containsKey(rowKey)) {
                return Collections.<Feature>emptyList();
              } else {
                Row row = rows.get(rowKey);
                return featureReferences.stream()
                    .map(FeatureReferenceV2::getFeatureTable)
                    .distinct()
                    .flatMap(
                        featureTableColumn -> {
                          ByteBuffer featureValues = row.getByteBuffer(featureTableColumn);
                          ByteBuffer schemaRefKey =
                              row.getByteBuffer(featureTableColumn + SCHEMA_REF_SUFFIX);

                          // Prevent retrieval of features from incorrect FeatureTable
                          List<FeatureReferenceV2> localFeatureReferences =
                              featureReferences.stream()
                                  .filter(
                                      featureReference ->
                                          featureReference
                                              .getFeatureTable()
                                              .equals(featureTableColumn))
                                  .collect(Collectors.toList());

                          List<Feature> features;
                          try {
                            features =
                                decodeFeatures(
                                    schemaRefKey,
                                    featureValues,
                                    localFeatureReferences,
                                    reusedDecoder,
                                    row.getLong(featureTableColumn + EVENT_TIMESTAMP_SUFFIX));
                          } catch (IOException e) {
                            throw new RuntimeException("Failed to decode features from Cassandra");
                          }

                          return features.stream();
                        })
                    .collect(Collectors.toList());
              }
            })
        .collect(Collectors.toList());
  }

  /**
   * Retrieve rows for each row entity key by generating Cassandra Query with filters based on
   * columns.
   *
   * @param tableName Name of Cassandra table
   * @param rowKeys List of keys of rows to retrieve
   * @param columnFamilies List of FeatureTable names
   * @return Map of retrieved features for each rowKey
   */
  @Override
  public Map<ByteBuffer, Row> getFeaturesFromSSTable(
      String tableName, List<ByteBuffer> rowKeys, List<String> columnFamilies) {
    List<String> schemaRefColumns =
        columnFamilies.stream().map(c -> c + SCHEMA_REF_SUFFIX).collect(Collectors.toList());
    Select query =
        QueryBuilder.selectFrom(tableName)
            .columns(columnFamilies)
            .columns(schemaRefColumns)
            .column(ENTITY_KEY);
    for (String columnFamily : columnFamilies) {
      query = query.writeTime(columnFamily).as(columnFamily + EVENT_TIMESTAMP_SUFFIX);
    }
    query = query.whereColumn(ENTITY_KEY).in(QueryBuilder.bindMarker());

    BoundStatement statement = session.prepare(query.build()).bind(rowKeys);

    return StreamSupport.stream(session.execute(statement).spliterator(), false)
        .collect(Collectors.toMap((Row row) -> row.getByteBuffer(ENTITY_KEY), Function.identity()));
  }

  private List<Feature> decodeFeatures(
      ByteBuffer schemaRefKey,
      ByteBuffer value,
      List<FeatureReferenceV2> featureReferences,
      BinaryDecoder reusedDecoder,
      long timestamp)
      throws IOException {

    CassandraSchemaRegistry.SchemaReference schemaReference =
        new CassandraSchemaRegistry.SchemaReference(schemaRefKey);

    // Convert ByteBuffer to ByteArray
    byte[] bytesArray = new byte[value.remaining()];
    value.get(bytesArray, 0, bytesArray.length);
    GenericDatumReader<GenericRecord> reader = schemaRegistry.getReader(schemaReference);
    reusedDecoder = DecoderFactory.get().binaryDecoder(bytesArray, reusedDecoder);
    GenericRecord record = reader.read(null, reusedDecoder);

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
}
