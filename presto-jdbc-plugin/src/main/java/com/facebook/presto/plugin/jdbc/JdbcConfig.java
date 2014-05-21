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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class JdbcConfig
{
    private String driverClass;
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;

    @NotNull
    public String getDriverClass()
    {
        return driverClass;
    }

    @Config("driver-class")
    public JdbcConfig setDriverClass(String driverClass)
    {
        this.driverClass = driverClass;
        return this;
    }

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public JdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    public JdbcConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    public JdbcConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }
}
