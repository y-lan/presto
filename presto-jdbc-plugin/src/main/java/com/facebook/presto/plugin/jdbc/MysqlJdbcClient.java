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
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.inject.Inject;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class MysqlJdbcClient
        implements JdbcClient
{
    private final String connectorId;
    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;

    @Inject
    public MysqlJdbcClient(JdbcConnectorId connectorId, JdbcConfig config)
            throws IOException
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();

        checkNotNull(config, "config is null");
        try {
            Class<? extends Driver> driverClass = getClass().getClassLoader().loadClass(config.getDriverClass()).asSubclass(Driver.class);
            driver = driverClass.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not load driver class " + config.getDriverClass());
        }

        connectionUrl = config.getConnectionUrl();

        connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.put("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.put("password", config.getConnectionPassword());
        }
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
             ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase();
                // skip the databases information_schema and sys schemas
                if (schemaName.equals("information_schema") || schemaName.equals("sys")) {
                    continue;
                }
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<String> getTableNames(String schema)
    {
        checkNotNull(schema, "schema is null");
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            if (metaData.storesUpperCaseIdentifiers()) {
                schema = schema.toUpperCase();
            }

            try (ResultSet resultSet = metaData.getTables(schema, null, null, null)) {
                ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
                while (resultSet.next()) {
                    tableNames.add(resultSet.getString(3).toLowerCase());
                }
                return tableNames.build();
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        checkNotNull(schemaTableName, "schemaTableName is null");
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metaData.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase();
                jdbcTableName = jdbcTableName.toUpperCase();
            }
            try (ResultSet resultSet = metaData.getTables(jdbcSchemaName, null, jdbcTableName, null)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(connectorId, schemaTableName, resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
                }
                if (tableHandles.isEmpty() || tableHandles.size() > 1) {
                    return null;
                }
                return Iterables.getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<ColumnMetadata> getColumns(JdbcTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getColumns(tableHandle.getCatalogName(), tableHandle.getSchemaName(), tableHandle.getTableName(), null)) {
                List<ColumnMetadata> columns = new ArrayList<>();
                int ordinalPosition = 0;
                while (resultSet.next()) {
                    Type type = toColumnType(resultSet.getInt(5));
                    // skip unsupported column types
                    if (type != null) {
                        String columnName = resultSet.getString(4).toLowerCase();
                        columns.add(new ColumnMetadata(columnName, type, ordinalPosition++, false));
                    }
                }
                if (columns.isEmpty()) {
                    return null;
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorPartitionResult getPartitions(JdbcTableHandle jdbcTableHandle, TupleDomain tupleDomain)
    {
        // currently we don't support partitions
        return new ConnectorPartitionResult(ImmutableList.<ConnectorPartition>of(new JdbcPartition(jdbcTableHandle)), tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(JdbcPartition jdbcPartition)
    {
        JdbcTableHandle jdbcTableHandle = jdbcPartition.getJdbcTableHandle();
        JdbcSplit jdbcSplit = new JdbcSplit(connectorId,
                jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                connectionUrl,
                Maps.fromProperties(connectionProperties));
        return new FixedSplitSource(connectorId, ImmutableList.of(jdbcSplit));
    }

    @Override
    public Connection getConnection(JdbcSplit split)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.putAll(split.getConnectionProperties());
        return driver.connect(split.getConnectionUrl(), properties);
    }

    @Override
    public String buildSql(JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        Joiner.on(", ").appendTo(sql, Iterables.transform(columnHandles, nameGetter()));
        sql.append(" FROM `").append(split.getCatalogName()).append("`.`").append(split.getTableName()).append("`");
        System.out.println(sql);
        return sql.toString();
    }

    private Type toColumnType(int jdbcType)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BooleanType.BOOLEAN;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return BigintType.BIGINT;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DoubleType.DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VarcharType.VARCHAR;
        }
        return null;
    }

    public static Function<JdbcColumnHandle, String> nameGetter()
    {
        return new Function<JdbcColumnHandle, String>()
        {
            @Override
            public String apply(JdbcColumnHandle columnHandle)
            {
                return "`" + columnHandle.getColumnName() + "`";
            }
        };
    }
}
