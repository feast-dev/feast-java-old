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
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;

public class CassandraSchemaRegistry {
  private final CqlSession session;
  private final LoadingCache<SchemaReference, Schema> cache;

  private static String KEYSPACE = "feast";
  private static String SCHEMA_COLUMN = "avro_schema";

  public static class SchemaReference {
    private final String tableName;
    private final ByteString schemaHash;

    public SchemaReference(String tableName, ByteString schemaHash) {
      this.tableName = tableName;
      this.schemaHash = schemaHash;
    }

    public String getTableName() {
      return tableName;
    }

    public ByteString getSchemaHash() {
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
    ResultSet rs =
        session.execute(
            String.format(
                "SELECT %s FROM %s.%s WHERE schema_ref = '%s'",
                SCHEMA_COLUMN,
                KEYSPACE,
                reference.getTableName(),
                ByteBuffer.wrap(reference.getSchemaHash().toByteArray())));
    Row row = rs.one();

    return new Schema.Parser().parse(row.getTupleValue(SCHEMA_COLUMN).toString());
  }
}
