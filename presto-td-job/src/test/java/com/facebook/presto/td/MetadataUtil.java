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

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

public final class MetadataUtil
{
    private MetadataUtil()
    {
    }

    //public static final JsonCodec<Map<String, List<TdjobTable>>> CATALOG_CODEC;
    public static final JsonCodec<TdjobListResult> JOB_LIST_CODEC;
    //public static final JsonCodec<> COLUMN_CODEC;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        //objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        //CATALOG_CODEC = codecFactory.mapJsonCodec(String.class, listJsonCodec(TdjobTable.class));
        //TABLE_CODEC = codecFactory.jsonCodec(TdjobTable.class);
        //COLUMN_CODEC = codecFactory.jsonCodec(TdjobColumnHandle.class);
        JOB_LIST_CODEC = codecFactory.jsonCodec(TdjobListResult.class);
    }

    /*
    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.of(
                BOOLEAN.getName(), BOOLEAN,
                BIGINT.getName(), BIGINT,
                DOUBLE.getName(), DOUBLE,
                VARCHAR.getName(), VARCHAR);

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase());
            if (type == null) {
                throw new IllegalArgumentException(String.valueOf("Unknown type " + value));
            }
            return type;
        }
    }
    */
}
