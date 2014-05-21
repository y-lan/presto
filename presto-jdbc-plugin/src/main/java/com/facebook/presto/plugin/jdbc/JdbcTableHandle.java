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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public final class JdbcTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public JdbcTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.schemaTableName = checkNotNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = checkNotNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
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

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(connectorId, schemaTableName);
    }

    @Override
    public final boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof JdbcTableHandle)) {
            return false;
        }
        final JdbcTableHandle other = (JdbcTableHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) &&
                Objects.equal(this.schemaTableName, other.schemaTableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").useForNull("null").join(connectorId, schemaTableName, catalogName, schemaName, tableName);
    }
}
