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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TdjobMetadata
        extends ReadOnlyConnectorMetadata
{
    private final String connectorId;

    private final TdjobClient tdjobClient;

    @Inject
    public TdjobMetadata(TdjobConnectorId connectorId, TdjobClient tdjobClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.tdjobClient = checkNotNull(tdjobClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(tdjobClient.getSchemaNames());
    }

    @Override
    public TdjobTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        TdjobTable table = tdjobClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new TdjobTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table)
    {
        checkArgument(table instanceof TdjobTableHandle, "tableHandle is not an instance of TdjobTableHandle");
        TdjobTableHandle tdjobTableHandle = (TdjobTableHandle) table;
        checkArgument(tdjobTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(tdjobTableHandle.getSchemaName(), tdjobTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = tdjobClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : tdjobClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public ConnectorColumnHandle getColumnHandle(ConnectorTableHandle tableHandle, String columnName)
    {
        return getColumnHandles(tableHandle).get(columnName);
    }

    @Override
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof TdjobTableHandle, "tableHandle is not an instance of TdjobTableHandle");
        TdjobTableHandle tdjobTableHandle = (TdjobTableHandle) tableHandle;
        checkArgument(tdjobTableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        TdjobTable table = tdjobClient.getTable(tdjobTableHandle.getSchemaName(), tdjobTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(tdjobTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : table.getColumnsMetadata()) {
            columnHandles.put(columnMetadata.getName(), new TdjobColumnHandle(connectorId, columnMetadata));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        TdjobTable table = tdjobClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof TdjobTableHandle, "tableHandle is not an instance of TdjobColumnHandle");
        checkArgument(((TdjobTableHandle) tableHandle).getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        checkArgument(columnHandle instanceof TdjobColumnHandle, "columnHandle is not an instance of TdjobColumnHandle");

        return ((TdjobColumnHandle) columnHandle).getColumnMetadata();
    }
}
