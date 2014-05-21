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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public final class TdjobColumn
{
    private final String name;
    private final Type type;
    private final String oriType;

    public TdjobColumn(String name, String type)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        checkArgument(!isNullOrEmpty(type), "type is null or is empty");
        this.name = name;
        this.oriType = type;
        this.type = toColumnType(type);
    }

    public String getName()
    {
        return name;
    }

    public String getOriType()
    {
        return oriType;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TdjobColumn other = (TdjobColumn) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type);
    }

    @Override
    public String toString()
    {
        return name + ":" + type;
    }

    private Type toColumnType(String tdType)
    {
        switch (tdType.toLowerCase()) {
            case "int":
            case "long":
            case "bigint":
                return BigintType.BIGINT;
            case "float":
            case "double":
                return DoubleType.DOUBLE;
            case "string":
            default:
                return VarcharType.VARCHAR;
        }
    }
}
