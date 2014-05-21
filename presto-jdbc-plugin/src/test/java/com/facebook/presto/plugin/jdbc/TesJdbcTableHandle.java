/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.SchemaTableName;
import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TesJdbcTableHandle
{
    private final JdbcTableHandle tableHandle = new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<JdbcTableHandle> codec = jsonCodec(JdbcTableHandle.class);
        String json = codec.toJson(tableHandle);
        JdbcTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalogX", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchemaX", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTableX"))
                .addEquivalentGroup(
                        new JdbcTableHandle("connectorIdX", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorIdX", new SchemaTableName("schema", "table"), "jdbcCatalogX", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorIdX", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchemaX", "jdbcTable"),
                        new JdbcTableHandle("connectorIdX", new SchemaTableName("schema", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTableX"))
                .addEquivalentGroup(
                        new JdbcTableHandle("connectorId", new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schemaX", "table"), "jdbcCatalogX", "jdbcSchema", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchemaX", "jdbcTable"),
                        new JdbcTableHandle("connectorId", new SchemaTableName("schemaX", "table"), "jdbcCatalog", "jdbcSchema", "jdbcTableX"))
                .check();
    }
}
