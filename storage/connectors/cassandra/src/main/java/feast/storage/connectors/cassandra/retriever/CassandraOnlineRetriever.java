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
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.google.protobuf.Timestamp;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.NativeFeature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

public class CassandraOnlineRetriever implements OnlineRetrieverV2 {

  private CqlSession session;
  private CassandraSchemaRegistry schemaRegistry;

  private static String ENTITY_KEY = "key";
  private static String SCHEMA_REF_KEY = "schema_ref";
  private static String TIMESTAMP_COLUMN = String.format("writetime(%s)", SCHEMA_REF_KEY);

  public CassandraOnlineRetriever(CqlSession session) {
    this.session = session;
    this.schemaRegistry = new CassandraSchemaRegistry(session);
  }

  /**
   * Generate name of Cassandra table in the form of <feastProject>__<entityNames>
   *
   * @param project Name of Feast project
   * @param entityNames List of entities used in retrieval call
   * @return Name of Cassandra table
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
   * Generate Cassandra key in the form of entity values joined by #.
   *
   * @param entityRow Single EntityRow representation in feature retrieval call
   * @param entityNames List of entities related to feature references in retrieval call
   * @return Cassandra key for retrieval
   */
  private ByteBuffer convertEntityValueToCassandraKey(
      ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow entityRow, List<String> entityNames) {
    return ByteBuffer.wrap(
        entityNames.stream()
            .map(entity -> entityRow.getFieldsMap().get(entity))
            .map(this::valueToString)
            .collect(Collectors.joining("#"))
            .getBytes());
  }

  /**
   * Retrieve Cassandra table column families based on FeatureTable names.
   *
   * @param featureReferences List of feature references of features in retrieval call
   * @return List of String of FeatureTable names
   */
  private List<String> getColumnFamilies(
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    return featureReferences.stream()
        .map(ServingAPIProto.FeatureReferenceV2::getFeatureTable)
        .collect(Collectors.toList());
  }

  private List<Feature> decodeFeatures(
      ByteBuffer schemaRefKey,
      ByteBuffer value,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      long timestamp)
      throws IOException {

    CassandraSchemaRegistry.SchemaReference schemaReference =
        new CassandraSchemaRegistry.SchemaReference(schemaRefKey);

    Schema schema = schemaRegistry.getSchema(schemaReference);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(value.array(), null);
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
      List<ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow> entityRows,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      List<String> entityNames) {

    List<String> columnFamilies = getColumnFamilies(featureReferences);
    String tableName = getTableName(project, entityNames);

    List<ByteBuffer> rowKeys =
        entityRows.stream()
            .map(row -> convertEntityValueToCassandraKey(row, entityNames))
            .collect(Collectors.toList());

    Map<ByteBuffer, Row> rowsFromCassandra =
        getFeaturesFromCassandra(tableName, rowKeys, columnFamilies);
    List<List<Feature>> features =
        convertRowToFeature(rowKeys, rowsFromCassandra, featureReferences, columnFamilies);

    return features;
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
  private Map<ByteBuffer, Row> getFeaturesFromCassandra(
      String tableName, List<ByteBuffer> rowKeys, List<String> columnFamilies) {
    SelectFrom query = QueryBuilder.selectFrom(String.format("\"%s\"", tableName));

    BoundStatement statement =
        session
            .prepare(
                query
                    .columns(columnFamilies)
                    .column(SCHEMA_REF_KEY)
                    .column(ENTITY_KEY)
                    .writeTime(SCHEMA_REF_KEY)
                    .whereColumn(ENTITY_KEY)
                    .in(QueryBuilder.bindMarker())
                    .build())
            .bind(rowKeys);

    return StreamSupport.stream(session.execute(statement).spliterator(), false)
        .collect(Collectors.toMap((Row row) -> row.getByteBuffer(ENTITY_KEY), Function.identity()));
  }

  /**
   * Converts rowCell feature value into @NativeFeature type.
   *
   * @param rowKeys List of keys of rows to retrieve
   * @param rows Map of rowKey to Row related to it
   * @param featureReferences List of feature references
   * @return List of List of Features associated with respective rowKey
   */
  private List<List<Feature>> convertRowToFeature(
      List<ByteBuffer> rowKeys,
      Map<ByteBuffer, Row> rows,
      List<ServingAPIProto.FeatureReferenceV2> featureReferences,
      List<String> columnFamilies) {

    return rowKeys.stream()
        .map(
            rowKey -> {
              if (!rows.containsKey(rowKey)) {
                return Collections.<Feature>emptyList();
              } else {
                Row row = rows.get(rowKey);

                String featureTableColumn = columnFamilies.get(0);
                ByteBuffer schemaRefKey = row.getByteBuffer(SCHEMA_REF_KEY);
                ByteBuffer featureValues = row.getByteBuffer(featureTableColumn);

                List<Feature> features;
                try {
                  features =
                      decodeFeatures(
                          schemaRefKey,
                          featureValues,
                          featureReferences,
                          row.getLong(TIMESTAMP_COLUMN));
                } catch (IOException e) {
                  throw new RuntimeException("Failed to decode features from Cassandra");
                }

                return features.stream().collect(Collectors.toList());
              }
            })
        .collect(Collectors.toList());
  }
}
