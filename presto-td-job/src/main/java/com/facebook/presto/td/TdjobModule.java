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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.configuration.ConfigurationModule.bindConfig;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class TdjobModule
        implements Module
{
    private final String connectorId;
    //private final TypeManager typeManager;

    public TdjobModule(String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connector id is null");
        //this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        //binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(TdjobConnector.class).in(Scopes.SINGLETON);
        binder.bind(TdjobConnectorId.class).toInstance(new TdjobConnectorId(connectorId));
        binder.bind(TdjobMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TdjobClient.class).in(Scopes.SINGLETON);
        binder.bind(TdjobSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TdjobRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(TdjobHandleResolver.class).in(Scopes.SINGLETON);
        bindConfig(binder).to(TdjobConfig.class);

        //jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        //jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(TdjobTable.class));

        jsonCodecBinder(binder).bindJsonCodec(TdjobListResult.class);
        jsonCodecBinder(binder).bindJsonCodec(TdjobTable.class);
    }

    /*
    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = checkNotNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(value);
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
    */
}
