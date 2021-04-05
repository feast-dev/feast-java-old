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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;

public class CassandraSchemaRegistry {
  private final CqlSession session;
  private final LoadingCache<SchemaReference, Schema> cache;

  private static String KEYSPACE = "feast";
  private static String SCHEMA_REF_TABLE = "feast_schema_reference";
  private static String SCHEMA_REF_COLUMN = "schema_ref";
  private static String SCHEMA_COLUMN = "avro_schema";

  public static class SchemaReference {
    private final ByteBuffer schemaHash;

    public SchemaReference(ByteBuffer schemaHash) {
      this.schemaHash = schemaHash;
    }

    public ByteBuffer getSchemaHash() {
      return schemaHash;
    }
  }

  public CassandraSchemaRegistry(CqlSession session) {
    this.session = session;

    CacheLoader<SchemaReference, Schema> schemaCacheLoader = CacheLoader.from(this::loadSchema);

    cache = CacheBuilder.newBuilder().build(schemaCacheLoader);
  }

  public Schema getSchema(SchemaReference reference) {
    Schema schema;
    try {
      schema = this.cache.get(reference);
    } catch (ExecutionException | CacheLoader.InvalidCacheLoadException e) {
      throw new RuntimeException(String.format("Unable to find Schema"), e);
    }
    return schema;
  }

  private Schema loadSchema(SchemaReference reference) {
    String tableName = String.format("%s.%s", KEYSPACE, SCHEMA_REF_TABLE);
    Select query =
        QueryBuilder.selectFrom(tableName)
            .column(SCHEMA_COLUMN)
            .whereColumn(SCHEMA_REF_COLUMN)
            .in(QueryBuilder.bindMarker());

    BoundStatement statement = session.prepare(query.build()).bind(reference.getSchemaHash());

    Row row = session.execute(statement).one();

    return new Schema.Parser().parse(row.getTupleValue(SCHEMA_COLUMN).toString());
  }
}
