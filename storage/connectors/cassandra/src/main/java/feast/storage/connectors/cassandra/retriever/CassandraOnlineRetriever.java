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
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.google.protobuf.ByteString;
import feast.proto.serving.ServingAPIProto;
import feast.proto.types.ValueProto;
import feast.storage.api.retriever.Feature;
import feast.storage.api.retriever.OnlineRetrieverV2;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CassandraOnlineRetriever implements OnlineRetrieverV2 {

  private CqlSession session;
  private CassandraSchemaRegistry schemaRegistry;

  private static String ENTITY_KEY = "key";

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
  private ByteString convertEntityValueToCassandraKey(
      ServingAPIProto.GetOnlineFeaturesRequestV2.EntityRow entityRow, List<String> entityNames) {
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
  private List<String> getColumnFamilies(
      List<ServingAPIProto.FeatureReferenceV2> featureReferences) {
    return featureReferences.stream()
        .map(ServingAPIProto.FeatureReferenceV2::getFeatureTable)
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
    List<ByteString> rowKeys =
        entityRows.stream()
            .map(row -> convertEntityValueToCassandraKey(row, entityNames))
            .collect(Collectors.toList());

    Map<ByteString, Row> rowsFromCassandra =
        getFeaturesFromCassandra(tableName, rowKeys, columnFamilies);

    return Collections.emptyList();
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
  private Map<ByteString, Row> getFeaturesFromCassandra(
      String tableName, List<ByteString> rowKeys, List<String> columnFamilies) {
    List<Selector> selectors =
        columnFamilies.stream().map(cf -> Selector.column(cf)).collect(Collectors.toList());

    Select query =
        QueryBuilder.selectFrom(tableName)
            .listOf(selectors)
            .whereColumn(ENTITY_KEY)
            .in(QueryBuilder.bindMarker());

    SimpleStatement statement = query.build();

    return Collections.emptyMap();
  }
}
