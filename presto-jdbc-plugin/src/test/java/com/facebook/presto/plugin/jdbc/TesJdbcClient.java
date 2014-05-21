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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.jdbc.TestingJdbcClient.createTestingJdbcClient;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TesJdbcClient
{
    private String catalogName;
    private JdbcClient jdbcClient;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        catalogName = "test" + System.nanoTime();
        jdbcClient = createTestingJdbcClient(catalogName);
    }

    @Test
    public void testMetadata()
            throws Exception
    {
        assertTrue(jdbcClient.getSchemaNames().containsAll(ImmutableSet.of("example", "tpch")));
        assertEquals(jdbcClient.getTableNames("example"), ImmutableSet.of("numbers"));
        assertEquals(jdbcClient.getTableNames("tpch"), ImmutableSet.of("orders", "lineitem"));

        SchemaTableName schemaTableName = new SchemaTableName("example", "numbers");
        JdbcTableHandle table = jdbcClient.getTableHandle(schemaTableName);
        assertNotNull(table, "table is null");
        assertEquals(table.getCatalogName(), catalogName.toUpperCase());
        assertEquals(table.getSchemaName(), "EXAMPLE");
        assertEquals(table.getTableName(), "NUMBERS");
        assertEquals(table.getSchemaTableName(), schemaTableName);
        assertEquals(jdbcClient.getColumns(table), ImmutableList.of(new ColumnMetadata("TEXT", VarcharType.VARCHAR, 0, false), new ColumnMetadata("VALUE", BigintType.BIGINT, 1, false)));
    }
}
