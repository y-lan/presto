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

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static com.facebook.presto.plugin.jdbc.ConditionalModule.installIfPropertyEquals;
import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcConnectorFactory
        implements ConnectorFactory
{
    private final Map<String, String> optionalConfig;
    private final ClassLoader classLoader;

    public JdbcConnectorFactory(Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
        this.classLoader = checkNotNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "jdbc";
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig)
    {
        checkNotNull(requiredConfig, "requiredConfig is null");
        checkNotNull(optionalConfig, "optionalConfig is null");

        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new JdbcModule(connectorId),
                    installIfPropertyEquals(new GenericJdbcClientModule(), "jdbc-client", "generic"),
                    installIfPropertyEquals(new MysqlJdbcClientModule(), "jdbc-client", "mysql"));

            Injector injector = app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(requiredConfig).setOptionalConfigurationProperties(optionalConfig).initialize();

            JdbcMetadata jdbcMetadata = injector.getInstance(JdbcMetadata.class);
            JdbcSplitManager jdbcSplitManager = injector.getInstance(JdbcSplitManager.class);
            JdbcRecordSetProvider jdbcRecordSetProvider = injector.getInstance(JdbcRecordSetProvider.class);
            JdbcHandleResolver jdbcHandleResolver = injector.getInstance(JdbcHandleResolver.class);

            return new JdbcConnector(jdbcMetadata, jdbcSplitManager, jdbcRecordSetProvider, jdbcHandleResolver);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
