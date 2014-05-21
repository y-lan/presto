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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class JdbcRecordCursor
        implements RecordCursor
{
    private final List<JdbcColumnHandle> columnHandles;

    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private boolean closed;

    public JdbcRecordCursor(JdbcClient jdbcClient, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;

        String sql = jdbcClient.buildSql(split, columnHandles);
        try {
            this.connection = jdbcClient.getConnection(split);
            statement = this.connection.createStatement();
            resultSet = statement.executeQuery(sql);
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed) {
            return false;
        }

        try {
            boolean result = resultSet.next();
            if (!result) {
                close();
            }
            return result;
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(getType(field) == BooleanType.BOOLEAN, "Expected field to be type %s but is %s", BooleanType.BOOLEAN, getType(field));
        try {
            return resultSet.getBoolean(field + 1);
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(getType(field) == BigintType.BIGINT, "Expected field to be type %s but is %s", BigintType.BIGINT, getType(field));
        try {
            return resultSet.getLong(field + 1);
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(getType(field) == DoubleType.DOUBLE, "Expected field to be type %s but is %s", DoubleType.DOUBLE, getType(field));
        try {
            return resultSet.getDouble(field + 1);
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(getType(field) == VarcharType.VARCHAR, "Expected field to be type %s but is %s", VarcharType.VARCHAR, getType(field));
        try {
            // converting from binary (in driver) to string and back to bytes
            // is not very efficient, but should be good enough for an example connector
            return Slices.utf8Slice(resultSet.getString(field + 1));
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.size(), "Invalid field index");

        try {
            // JDBC is kind of dumb, we need to read the field and then ask if it was null, which
            // means we are wasting effort here.  We could safe off the results of the field access
            // if it maters.
            switch (getType(field).getName()) {
                case "boolean":
                    resultSet.getBoolean(field + 1);
                    break;
                case "bigint":
                    resultSet.getLong(field + 1);
                    break;
                case "double":
                    resultSet.getDouble(field + 1);
                    break;
                case "varchar":
                    resultSet.getString(field + 1);
                    break;
            }
            return resultSet.wasNull();
        }
        catch (SQLException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public void close()
    {
        closed = true;

        // use try with resources to close everything properly
        //noinspection EmptyTryBlock
        try (ResultSet resultSet = this.resultSet;
             Statement statement = this.statement;
             Connection connection = this.connection) {
            // do nothing
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private RuntimeException handleSqlException(SQLException e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            e.addSuppressed(closeException);
        }
        return Throwables.propagate(e);
    }
}
