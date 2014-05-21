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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.plugin.jdbc.TestingJdbcClient.createTestingJdbcClient;
import static org.testng.Assert.assertEquals;

public class TesJdbcRecordSetProvider
{
    private JdbcClient jdbcClient;
    private JdbcSplit split;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        jdbcClient = createTestingJdbcClient("test" + System.nanoTime());
        split = TestingJdbcClient.getSplit(jdbcClient, "example", "numbers");
    }

    /*
    @Test
    public void testCanHandle()
    {
        JdbcRecordSetProvider recordSetProvider = new JdbcRecordSetProvider(new JdbcConnectorId("test"), jdbcClient);
        assertTrue(recordSetProvider.canHandle(split));
        assertFalse(recordSetProvider.canHandle(new JdbcSplit("unknown", "catalog", "schema", "table", "connectionUrl", ImmutableMap.<String, String>of())));
    }
    */

    @Test
    public void testGetRecordSet()
            throws Exception
    {
        JdbcRecordSetProvider recordSetProvider = new JdbcRecordSetProvider(new JdbcConnectorId("test"), jdbcClient);
        RecordSet recordSet = recordSetProvider.getRecordSet(split, ImmutableList.of(
                new JdbcColumnHandle("test", "text", VarcharType.VARCHAR, 0),
                new JdbcColumnHandle("test", "value", BigintType.BIGINT, 1)));
        Assert.assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        Assert.assertNotNull(cursor, "cursor is null");

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("one", 1L)
                .put("two", 2L)
                .put("three", 3L)
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .build());
    }
}
