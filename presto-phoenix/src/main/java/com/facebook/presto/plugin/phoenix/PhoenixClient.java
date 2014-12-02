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
package com.facebook.presto.plugin.phoenix;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.phoenix.jdbc.PhoenixDriver;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Locale.ENGLISH;

public class PhoenixClient
        extends BaseJdbcClient
{
    public static final String DEFAULT_SCHEM = "DEFAULT";

    @Inject
    public PhoenixClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new PhoenixDriver());
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
             ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = extractSchem(resultSet);
                // skip internal schemas
                if (!schemaName.toLowerCase().equals("system")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        return connection.getMetaData().getTables(connection.getCatalog(), asRealSChem(schemaName), tableName, new String[]{"TABLE", "VIEW"});
    }

    protected ResultSet getTables(Connection connection, String schemaName, String tableName, String tableType)
            throws SQLException
    {
        return connection.getMetaData().getTables(connection.getCatalog(), asRealSChem(schemaName), tableName, new String[]{tableType});
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName, "TABLE")) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    resultSet.close();
                    try (ResultSet resultSet2 = getTables(connection, jdbcSchemaName, schemaTableName.getTableName(), "VIEW")) {
                        while (resultSet2.next()) {
                            tableHandles.add(new JdbcTableHandle(
                                    connectorId,
                                    schemaTableName,
                                    resultSet2.getString("TABLE_CAT"),
                                    resultSet2.getString("TABLE_SCHEM"),
                                    resultSet2.getString("TABLE_NAME")));
                        }
                    }
                    if (tableHandles.isEmpty()) {
                        return null;
                    }
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        return new SchemaTableName(
                extractSchem(resultSet),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    protected String extractSchem(ResultSet resultSet) throws SQLException
    {
        String schem = resultSet.getString("TABLE_SCHEM");
        if (schem == null) {
            return DEFAULT_SCHEM.toLowerCase(ENGLISH);
        }
        else {
            return schem.toLowerCase(ENGLISH);
        }
    }

    protected String asRealSChem(String schemaName)
    {
        if (schemaName == null) {
            return null;
        }
        else if (schemaName.toUpperCase().equals(DEFAULT_SCHEM)) {
            return "";
        }
        else {
            return schemaName;
        }
    }
}
