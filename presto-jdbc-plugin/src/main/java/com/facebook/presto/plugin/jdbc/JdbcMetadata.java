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

public class JdbcMetadata
        extends ReadOnlyConnectorMetadata
{
    private final String connectorId;

    private final JdbcClient jdbcClient;

    @Inject
    public JdbcMetadata(JdbcConnectorId connectorId, JdbcClient jdbcClient)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.jdbcClient = checkNotNull(jdbcClient, "client is null");
    }

    //@Override
    public boolean canHandle(ConnectorTableHandle tableHandle)
    {
        return tableHandle instanceof JdbcTableHandle && ((JdbcTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(jdbcClient.getSchemaNames());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return jdbcClient.getTableHandle(schemaTableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table)
    {
        checkArgument(table instanceof JdbcTableHandle, "tableHandle is not an instance of ExampleTableHandle");
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) table;

        List<ColumnMetadata> columns = jdbcClient.getColumns(jdbcTableHandle);
        if (columns == null) {
            return null;
        }

        return new ConnectorTableMetadata(jdbcTableHandle.getSchemaTableName(), columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = jdbcClient.getSchemaNames();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : jdbcClient.getTableNames(schemaName)) {
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
        checkArgument(tableHandle instanceof JdbcTableHandle, "tableHandle is not an instance of ExampleTableHandle");
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableHandle;

        List<ColumnMetadata> columns = jdbcClient.getColumns(jdbcTableHandle);
        if (columns == null) {
            throw new TableNotFoundException(jdbcTableHandle.getSchemaTableName());
        }

        ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : columns) {
            columnHandles.put(columnMetadata.getName(), new JdbcColumnHandle(connectorId, columnMetadata));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(prefix)) {
            try {
                JdbcTableHandle tableHandle = jdbcClient.getTableHandle(tableName);
                ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
                if (tableMetadata == null) {
                    continue;
                }
                columns.put(tableName, tableMetadata.getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    public List<SchemaTableName> listTables(SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(prefix);
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(tableHandle instanceof JdbcTableHandle, "tableHandle is not an instance of ExampleTableHandle");
        checkArgument(columnHandle instanceof JdbcColumnHandle, "columnHandle is not an instance of ExampleColumnHandle");

        return ((JdbcColumnHandle) columnHandle).getColumnMetadata();
    }
}
