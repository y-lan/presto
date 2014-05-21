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
package com.facebook.presto.td;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static com.facebook.presto.td.MetadataUtil.JOB_LIST_CODEC;
import static org.testng.Assert.assertEquals;

public class TestTdJobClient
{
    @Test
    public void testMetadata()
            throws Exception
    {
        TdjobClient client = new TdjobClient(new TdjobConfig().setMetadataKeys("dig:52e2a9e2e9399c1ad1632544ccb03c1d959ba5bb, gundam:170d4e0cd09bb44a5b577e0fd0554dc8aade6000"), JOB_LIST_CODEC);
        assertEquals(client.getSchemaNames(), ImmutableSet.of("dig", "gundam"));
        assertEquals(client.getTableNames("dig").size(), 100);
        assertEquals(client.getTableNames("gundam").size(), 100);

        /*
        assertEquals(client.getTable("dig", "10580089").getColumns(), ImmutableList.of(
                new TdjobColumn("day", "string"),
                new TdjobColumn("hour", "bigint"),
                new TdjobColumn("type", "string"),
                new TdjobColumn("name", "string"),
                new TdjobColumn("revenue", "bigint"),
                new TdjobColumn("paid_uu", "bigint")
        ));
        */

        TdjobTable dig = client.getTable("dig", "10576300");

        System.out.println(JsonCodec.jsonCodec(TdjobTable.class).toJson(dig));

        String s = "{\\\"session\\\":{\\\"user\\\":\\\"yuyang-lan\\\",\\\"source\\\":\\\"presto-cli\\\",\\\"catalog\\\":\\\"default\\\",\\\"schema\\\":\\\"default\\\",\\\"timeZoneKey\\\":1989,\\\"locale\\\":\\\"en_US\\\",\\\"remoteUserAddress\\\":\\\"127.0.0.1\\\",\\\"userAgent\\\":\\\"StatementClient/0.61-SNAPSHOT\\\",\\\"startTime\\\":1400554060773},\\\"fragment\\\":{\\\"id\\\":\\\"0\\\",\\\"root\\\":{\\\"type\\\":\\\"sink\\\",\\\"id\\\":\\\"6\\\",\\\"source\\\":{\\\"type\\\":\\\"tablescan\\\",\\\"id\\\":\\\"0\\\",\\\"table\\\":{\\\"connectorId\\\":\\\"tdjob\\\",\\\"connectorHandle\\\":{\\\"type\\\":\\\"tdjob\\\",\\\"connectorId\\\":\\\"tdjob\\\",\\\"schemaName\\\":\\\"dig\\\",\\\"tableName\\\":\\\"10597915\\\"}},\\\"outputSymbols\\\":[\\\"segment\\\",\\\"day\\\",\\\"uu\\\"],\\\"assignments\\\":{\\\"segment\\\":{\\\"connectorId\\\":\\\"tdjob\\\",\\\"connectorHandle\\\":{\\\"type\\\":\\\"tdjob\\\",\\\"connectorId\\\":\\\"tdjob\\\",\\\"columnName\\\":\\\"segment\\\",\\\"columnType\\\":\\\"varchar\\\",\\\"ordinalPosition\\\":0}},\\\"day\\\":{\\\"connectorId\\\":\\\"tdjob\\\",\\\"connectorHandle\\\":{\\\"type\\\":\\\"tdjob\\\",\\\"connectorId\\\":\\\"tdjob\\\",\\\"columnName\\\":\\\"day\\\",\\\"columnType\\\":\\\"varchar\\\",\\\"ordinalPosition\\\":1}},\\\"uu\\\":{\\\"connectorId\\\":\\\"tdjob\\\",\\\"connectorHandle\\\":{\\\"type\\\":\\\"tdjob\\\",\\\"connectorId\\\":\\\"tdjob\\\",\\\"columnName\\\":\\\"uu\\\",\\\"columnType\\\":\\\"bigint\\\",\\\"ordinalPosition\\\":2}}},\\\"originalConstraint\\\":\\\"true\\\",\\\"partitionDomainSummary\\\":\\\"TupleDomain:ALL\\\"},\\\"outputSymbols\\\":[\\\"segment\\\",\\\"day\\\",\\\"uu\\\"]},\\\"symbols\\\":{\\\"segment\\\":\\\"varchar\\\",\\\"uu\\\":\\\"bigint\\\",\\\"day\\\":\\\"varchar\\\"},\\\"distribution\\\":\\\"SOURCE\\\",\\\"partitionedSource\\\":\\\"0\\\",\\\"outputPartitioning\\\":\\\"NONE\\\",\\\"partitionBy\\\":[]},\\\"sources\\\":[{\\\"planNodeId\\\":\\\"0\\\",\\\"splits\\\":[{\\\"sequenceId\\\":0,\\\"split\\\":{\\\"connectorId\\\":\\\"tdjob\\\",\\\"connectorSplit\\\":{\\\"type\\\":\\\"tdjob\\\",\\\"connectorId\\\":\\\"tdjob\\\",\\\"schemaName\\\":\\\"dig\\\",\\\"tableName\\\":\\\"10597915\\\"}}}],\\\"noMoreSplits\\\":false}],\\\"outputIds\\\":{\\\"version\\\":0,\\\"noMoreBufferIds\\\":false,\\\"buffers\\\":{}}}";

        /*
        ExampleTable table = client.getTable("example", "numbers");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "numbers");
        assertEquals(table.getColumns(), ImmutableList.of(new ExampleColumn("text", VARCHAR), new ExampleColumn("value", BIGINT)));
        assertEquals(table.getSources(), ImmutableList.of(metadata.resolve("numbers-1.csv"), metadata.resolve("numbers-2.csv")));
        */
    }
}
