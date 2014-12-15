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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.operator.scalar.JsonExtract.generateExtractor;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class RowJsonGet
        extends ParametricScalar
{
    public static final RowJsonGet ROW_JSON_GET = new RowJsonGet();
    private static final Signature SIGNATURE = new Signature("json_get", ImmutableList.of(withVariadicBound("T", "row"), typeParameter("E")), "E", ImmutableList.of("T", StandardTypes.VARCHAR), false, false);
    private static final Map<String, MethodHandle> METHOD_HANDLE_MAP;

    static {
        ImmutableMap.Builder<String, MethodHandle> builder = ImmutableMap.builder();
        builder.put("long", methodHandle(RowJsonGet.class, "longAccessor", JsonExtract.JsonExtractor.class, Slice.class, Slice.class));
        builder.put("double", methodHandle(RowJsonGet.class, "doubleAccessor", JsonExtract.JsonExtractor.class, Slice.class, Slice.class));
        builder.put("boolean", methodHandle(RowJsonGet.class, "booleanAccessor", JsonExtract.JsonExtractor.class, Slice.class, Slice.class));
        builder.put("slice", methodHandle(RowJsonGet.class, "sliceAccessor", JsonExtract.JsonExtractor.class, Slice.class, Slice.class));
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
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        return specialize(types, arity, typeManager, functionRegistry);
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, List<TypeSignature> typeSignatures, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(typeSignatures.size() == 2, "Expected arity to be 2");
        Type type = types.get("T");
        checkArgument(typeSignatures.size() == 2 && typeSignatures.get(1).getLiteralValue() != null, "Expected the 2nd parameter to be literal string");

        Object o = typeSignatures.get(1).getLiteralValue();
        if ((o instanceof String || o instanceof Slice) && type instanceof RowType) {
            RowType rowType = (RowType) type;
            final String fieldName = (o instanceof String) ? (String) o : ((Slice) o).toStringUtf8();
            int index = Iterables.indexOf(rowType.getFields(), new Predicate<RowType.RowField>()
            {
                @Override
                public boolean apply(RowType.RowField input)
                {
                    return input.getName().equals(fieldName);
                }
            });

            Type returnType = rowType.getFields().get(index).getType();
            JsonExtract.JsonExtractor<?> extractor;
            if (returnType instanceof ArrayType || returnType instanceof MapType || returnType instanceof RowType) {
                extractor = new JsonExtract.JsonValueJsonExtractor();
            }
            else if (returnType.getJavaType() == boolean.class) {
                extractor = new JsonExtract.BooleanJsonExtractor();
            }
            else if (returnType.getJavaType() == long.class) {
                extractor = new JsonExtract.LongJsonExtractor();
            }
            else if (returnType.getJavaType() == double.class) {
                extractor = new JsonExtract.DoubleJsonExtractor();
            }
            else if (returnType.getJavaType() == Slice.class) {
                extractor = new JsonExtract.ScalarValueJsonExtractor();
            }
            else {
                throw new IllegalArgumentException("Unsupported stack type: " + returnType.getJavaType());
            }
            extractor = generateExtractor(format("$[%d]", index), extractor, true);
            String stackType = returnType.getJavaType().getSimpleName().toLowerCase();
            checkState(METHOD_HANDLE_MAP.containsKey(stackType), "method handle missing for %s stack type", stackType);
            return new FunctionInfo(new Signature("json_get", returnType.getTypeSignature(), typeSignatures)
                    , getDescription(), isHidden(), METHOD_HANDLE_MAP.get(stackType).bindTo(extractor), true, true, ImmutableList.of(false, false));
        }

        return null;
    }

    public static Long longAccessor(JsonExtract.JsonExtractor<Long> extractor, Slice row, Slice key)
    {
        return JsonExtract.extract(row, extractor);
    }

    public static Boolean booleanAccessor(JsonExtract.JsonExtractor<Boolean> extractor, Slice row, Slice key)
    {
        return JsonExtract.extract(row, extractor);
    }

    public static Double doubleAccessor(JsonExtract.JsonExtractor<Double> extractor, Slice row, Slice key)
    {
        return JsonExtract.extract(row, extractor);
    }

    public static Slice sliceAccessor(JsonExtract.JsonExtractor<Slice> extractor, Slice row, Slice key)
    {
        return JsonExtract.extract(row, extractor);
    }
}
