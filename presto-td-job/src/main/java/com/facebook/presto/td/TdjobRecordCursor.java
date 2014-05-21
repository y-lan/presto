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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.io.InputSupplier;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.msgpack.MessagePack;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class TdjobRecordCursor
        implements RecordCursor
{
    private final List<TdjobColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;
    private final long totalBytes;
    private long completeBytes = 0L;

    private final InputStream input;
    private final Unpacker unpacker;
    private final UnpackerIterator iterator;

    private ArrayValue fields;
    private boolean closed;

    public TdjobRecordCursor(
            List<TdjobColumnHandle> columnHandles,
            long totalBytes,
            InputSupplier<InputStream> inputStreamSupplier)
    {
        this.columnHandles = columnHandles;
        this.totalBytes = totalBytes;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            TdjobColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        try {
            this.input = new GZIPInputStream(inputStreamSupplier.getInput());
            this.unpacker = new MessagePack().createUnpacker(input);
            this.iterator = unpacker.iterator();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
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

        if (!iterator.hasNext()) {
            completeBytes = totalBytes;
            return false;
        }

        fields = (ArrayValue) iterator.next();
        completeBytes = unpacker.getReadByteCount();

        return true;
    }

    private Value getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yes");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return getFieldValue(field).asBooleanValue().getBoolean();
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return getFieldValue(field).asIntegerValue().getLong();
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return getFieldValue(field).asFloatValue().getDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field).toString());
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getFieldValue(field).isNilValue();
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
        closed = true;
        // use try with resources to close everything properly
        //noinspection EmptyTryBlock
        try (Unpacker unpacker = this.unpacker;
             InputStream inputStream = this.input;) {
            // do nothing
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

    }
}
