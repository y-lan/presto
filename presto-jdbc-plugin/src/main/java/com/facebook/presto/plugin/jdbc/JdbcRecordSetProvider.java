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

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final JdbcClient jdbcClient;

    @Inject
    public JdbcRecordSetProvider(JdbcConnectorId connectorId, JdbcClient jdbcClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.jdbcClient = checkNotNull(jdbcClient, "jdbcClient is null");
    }

    /*
    @Override
    public boolean canHandle(ConnectorSplit split)
    {
        return split instanceof JdbcSplit && ((JdbcSplit) split).getConnectorId().equals(connectorId);
    }
    */

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ConnectorColumnHandle> columns)
    {
        checkNotNull(split, "partitionChunk is null");
        checkArgument(split instanceof JdbcSplit);

        JdbcSplit jdbcSplit = (JdbcSplit) split;
        ImmutableList.Builder<JdbcColumnHandle> handles = ImmutableList.builder();
        for (ConnectorColumnHandle handle : columns) {
            checkArgument(handle instanceof JdbcColumnHandle);
            handles.add((JdbcColumnHandle) handle);
        }

        return new JdbcRecordSet(jdbcClient, jdbcSplit, handles.build());
    }
}
