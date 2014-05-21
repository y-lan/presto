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

import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcPartition
        implements ConnectorPartition
{
    private final JdbcTableHandle jdbcTableHandle;

    public JdbcPartition(JdbcTableHandle jdbcTableHandle)
    {
        this.jdbcTableHandle = checkNotNull(jdbcTableHandle, "jdbcTableHandle is null");
    }

    @Override
    public String getPartitionId()
    {
        return jdbcTableHandle.toString();
    }

    public JdbcTableHandle getJdbcTableHandle()
    {
        return jdbcTableHandle;
    }

    @Override
    public TupleDomain getTupleDomain()
    {
        return TupleDomain.all();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("jdbcTableHandle", jdbcTableHandle)
                .toString();
    }
}
