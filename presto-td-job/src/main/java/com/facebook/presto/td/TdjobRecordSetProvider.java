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
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TdjobRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final TdjobClient tdjobClient;

    @Inject
    public TdjobRecordSetProvider(TdjobConnectorId connectorId, TdjobClient tdjobClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.tdjobClient = checkNotNull(tdjobClient, "tdjobClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {
        checkNotNull(split, "partitionChunk is null");
        checkArgument(split instanceof TdjobSplit);

        TdjobSplit tdjobSplit = (TdjobSplit) split;
        checkArgument(tdjobSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<TdjobColumnHandle> handles = ImmutableList.builder();
        for (ConnectorColumnHandle handle : columns) {
            checkArgument(handle instanceof TdjobColumnHandle);
            handles.add((TdjobColumnHandle) handle);
        }

        return new TdjobRecordSet(tdjobClient, tdjobSplit, handles.build());
    }
}
