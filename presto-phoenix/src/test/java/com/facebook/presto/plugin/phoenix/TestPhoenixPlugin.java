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
package com.facebook.presto.plugin.phoenix;

import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TestPhoenixPlugin
{
    private void print(ResultSet resultSet) throws SQLException
    {
        ResultSetMetaData metaData = resultSet.getMetaData();

        int count = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            if (i != 1) {
                sb.append(",");
            }
            sb.append(metaData.getColumnName(i).toLowerCase());
            sb.append(":");
            sb.append(metaData.getColumnTypeName(i).toLowerCase());
        }
        System.out.println(sb.toString());

        while (resultSet.next()) {
            sb.setLength(0);
            for (int i = 1; i <= count; i++) {
                if (sb.length() != 0) {
                    sb.append(",");
                }

                sb.append(resultSet.getString(i));
            }
            System.out.println(sb.toString());
        }
        System.out.println("\n");
    }

    @Test
    public void testCreateConnector()
            throws Exception
    {
        System.out.println("Good Morning");

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        String connectionURL = "jdbc:phoenix:localhost:2181";
        Connection connection = DriverManager.getConnection(connectionURL);
        System.out.println("catalogs");
        print(connection.getMetaData().getCatalogs());
        System.out.println("schemas");
        print(connection.getMetaData().getSchemas());
        System.out.println("tables");
        print(connection.getMetaData().getTables(null, "", null, null));

        connection.close();
    }
}
