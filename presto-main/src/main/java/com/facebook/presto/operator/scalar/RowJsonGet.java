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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.RowType;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class RowJsonGet
        extends ParametricScalar
{
    public static final RowJsonGet ROW_JSON_GET = new RowJsonGet();
    private static final Signature SIGNATURE = new Signature("json_get", ImmutableList.of(withVariadicBound("T", "row"), typeParameter("E")), "E", ImmutableList.of("T", StandardTypes.VARCHAR), false, false);
    private static final Map<String, MethodHandle> METHOD_HANDLE_MAP;
    //private static final MethodHandle METHOD_HANDLE = methodHandle(RowJsonGet.class, "jsonGet", Type.class, ConnectorSession.class, Slice.class, Slice.class);

    static {
        ImmutableMap.Builder<String, MethodHandle> builder = ImmutableMap.builder();
        builder.put("long", methodHandle(RowJsonGet.class, "extractLong", Type.class, ConnectorSession.class, Slice.class, Slice.class));
        builder.put("slice", methodHandle(RowJsonGet.class, "extractSlice", Type.class, ConnectorSession.class, Slice.class, Slice.class));
        METHOD_HANDLE_MAP = builder.build();
    }

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        checkArgument(arity == 2, "Expected arity to be 2");
        Type type = types.get("T");
        Type retType = types.get("E");

        MethodHandle methodHandle = null;
        if (retType.equals(VarcharType.VARCHAR)) {
            methodHandle = METHOD_HANDLE_MAP.get("slice").bindTo(type);
        }
        else if (retType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_MAP.get("long").bindTo(type);
        }
        if (methodHandle != null) {
            return new FunctionInfo(new Signature("json_get", retType.getTypeSignature(),
                    ImmutableList.of(type.getTypeSignature(), VarcharType.VARCHAR.getTypeSignature())),
                    getDescription(), isHidden(), methodHandle, true, true, ImmutableList.of(false, false));
        }

        return null;
    }

    private static Object extract(Type rowType, ConnectorSession session, Slice row, Slice key)
    {
        final String f = key.toStringUtf8();
        int i = Iterables.indexOf(((RowType) rowType).getFields(), new Predicate<RowType.RowField>()
        {
            @Override
            public boolean apply(RowType.RowField input)
            {
                return input.getName().equals(f);
            }
        });
        return ((List) stackRepresentationToObject(session, row, rowType)).get(i);
    }

    public static Long extractLong(Type rowType, ConnectorSession session, Slice row, Slice key)
    {
        return (Long) extract(rowType, session, row, key);
    }

    public static Slice extractSlice(Type rowType, ConnectorSession session, Slice row, Slice key)
    {
        return Slices.utf8Slice(extract(rowType, session, row, key).toString());
    }
}
