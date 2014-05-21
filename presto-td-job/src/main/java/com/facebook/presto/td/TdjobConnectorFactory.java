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

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TdjobConnectorFactory
        implements ConnectorFactory
{
    //private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;

    public TdjobConnectorFactory(Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        //this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "tdjob";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig)
    {
        checkNotNull(requiredConfig, "requiredConfig is null");
        checkNotNull(optionalConfig, "optionalConfig is null");

        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new TdjobModule(connectorId));

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(TdjobConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
