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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TdjobSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final TdjobClient tdjobClient;

    @Inject
    public TdjobSplitManager(TdjobConnectorId connectorId, TdjobClient tdjobClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.tdjobClient = checkNotNull(tdjobClient, "client is null");
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        checkArgument(tableHandle instanceof TdjobTableHandle, "tableHandle is not an instance of TdjobTableHandle");
        TdjobTableHandle tdTableHandle = (TdjobTableHandle) tableHandle;

        // has only one partition
        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(new TdjobPartition(tdTableHandle.getSchemaName(), tdTableHandle.getTableName()));
        // does not do any additional processing/filtering with the TupleDomain, so just return the whole TupleDomain
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        checkArgument(partitions.size() == 1, "Expected one partition but got %s", partitions.size());
        ConnectorPartition partition = partitions.get(0);

        checkArgument(partition instanceof TdjobPartition, "partition is not an instance of tdjobPartition");
        TdjobPartition tdjobPartition = (TdjobPartition) partition;

        TdjobTableHandle tdjobTableHandle = (TdjobTableHandle) tableHandle;
        TdjobTable table = tdjobClient.getTable(tdjobTableHandle.getSchemaName(), tdjobTableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tdjobTableHandle.getSchemaName(), tdjobTableHandle.getTableName());

        List<ConnectorSplit> splits = Lists.newArrayList();
        String schemaName = tdjobPartition.getSchemaName();
        String tableName = tdjobPartition.getTableName();
        //for (URI uri : table.getSources()) {
        splits.add(new TdjobSplit(connectorId, schemaName, tableName,
                tdjobClient.getTable(schemaName, tableName).getResultSize()));
        //}
        //Collections.shuffle(splits);

        return new FixedSplitSource(connectorId, splits);
    }
}
