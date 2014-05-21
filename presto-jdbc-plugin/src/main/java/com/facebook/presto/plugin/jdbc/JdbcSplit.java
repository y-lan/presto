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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final String connectionUrl;
    private final Map<String, String> connectionProperties;

    @JsonCreator
    public JdbcSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("connectionUrl") String connectionUrl,
            @JsonProperty("connectionProperties") Map<String, String> connectionProperties)
    {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        this.tableName = checkNotNull(tableName, "table name is null");
        this.connectionUrl = checkNotNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = ImmutableMap.copyOf(checkNotNull(connectionProperties, "connectionProperties is null"));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @JsonProperty
    public Map<String, String> getConnectionProperties()
    {
        return connectionProperties;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
