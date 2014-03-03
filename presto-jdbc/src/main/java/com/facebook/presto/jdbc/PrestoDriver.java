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
package com.facebook.presto.jdbc;

import com.google.common.base.Throwables;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;

public class PrestoDriver
        implements Driver, Closeable
{
    static final int VERSION_MAJOR = 1;
    static final int VERSION_MINOR = 0;

    static final int JDBC_VERSION_MAJOR = 4;
    static final int JDBC_VERSION_MINOR = 1;

    static final String DRIVER_NAME = "Presto JDBC Driver";
    static final String DRIVER_VERSION = VERSION_MAJOR + "." + VERSION_MINOR;

    private static final DriverPropertyInfo[] DRIVER_PROPERTY_INFOS = {};

    private static final String JDBC_URL_START = "jdbc:";
    private static final String DRIVER_URL_START = "jdbc:presto:";

    private static final String USER_PROPERTY = "user";
    private static final String PASSWORD_PROPERTY = "password";
    private static final String SECURE_PROPERTY = "secure";

    private final QueryExecutor queryExecutor;

    static {
        try {
            DriverManager.registerDriver(new PrestoDriver());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public PrestoDriver()
    {
        this.queryExecutor = QueryExecutor.create(DRIVER_NAME + "/" + DRIVER_VERSION);
    }

    @Override
    public void close()
    {
        queryExecutor.close();
    }

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        String user = info.getProperty(USER_PROPERTY);
        if (isNullOrEmpty(user)) {
            throw new SQLException(format("Username property (%s) must be set", USER_PROPERTY));
        }
        String password = info.getProperty(PASSWORD_PROPERTY);
        if (isNullOrEmpty(password)) {
            throw new SQLException(format("Password property (%s) must be set", PASSWORD_PROPERTY));
        }

        URI uri = parseDriverUrl(url);
        Map<String, String> qs = null;
        try {
            qs = parseUriQueryString(uri);
        }
        catch (UnsupportedEncodingException e) {
            throw new SQLException(format("Connection URI (%s) cannot be parsed", uri.toString()));
        }

        boolean secure = ((qs != null) && (qs.containsKey(SECURE_PROPERTY))) ? Boolean.valueOf(qs.get(SECURE_PROPERTY)) : true;

        PrestoConnection connection = new PrestoConnection(uri, secure, user, password, queryExecutor);

        try {
            connection.createStatement().executeQuery("show catalogs");
        }
        catch (SQLException e) {
            throw new SQLException("Error in setting up connection. May be caused by wrong username / password");
        }

        return connection;
    }

    private static Map<String, String> parseUriQueryString(URI uri) throws UnsupportedEncodingException
    {
        Map<String, String> queryPairs = new LinkedHashMap<>();
        String query = uri.getQuery();
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            queryPairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
        }
        return queryPairs;
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        return url.startsWith(DRIVER_URL_START);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        return DRIVER_PROPERTY_INFOS;
    }

    @Override
    public int getMajorVersion()
    {
        return VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion()
    {
        return VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant()
    {
        // TODO: pass compliance tests
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        // TODO: support java.util.Logging
        throw new SQLFeatureNotSupportedException();
    }

    private static URI parseDriverUrl(String url)
            throws SQLException
    {
        URI uri;
        try {
            uri = new URI(url.substring(JDBC_URL_START.length()));
        }
        catch (URISyntaxException e) {
            throw new SQLException("Invalid JDBC URL: " + url, e);
        }
        if (isNullOrEmpty(uri.getHost())) {
            throw new SQLException("No host specified: " + url);
        }
        if (uri.getPort() == -1) {
            throw new SQLException("No port number specified: " + url);
        }
        if ((uri.getPort() < 1) || (uri.getPort() > 65535)) {
            throw new SQLException("Invalid port number: " + url);
        }
        return uri;
    }
}
