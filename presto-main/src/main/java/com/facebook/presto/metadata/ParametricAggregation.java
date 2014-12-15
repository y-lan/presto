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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;
import java.util.Map;

public abstract class ParametricAggregation
        implements ParametricFunction
{
    @Override
    public final boolean isScalar()
    {
        return false;
    }

    @Override
    public final boolean isAggregate()
    {
        return true;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isApproximate()
    {
        return false;
    }

    @Override
    public final boolean isWindow()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public final boolean isUnbound()
    {
        return true;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, List<TypeSignature> typeSignatures, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        return specialize(types, typeSignatures.size(), typeManager, functionRegistry);
    }
}
