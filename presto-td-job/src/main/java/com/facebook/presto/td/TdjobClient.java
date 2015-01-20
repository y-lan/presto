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

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.td.TdjobTable.nameGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;

public class TdjobClient
{
    public static final String TD_API_PREFIX = "http://api.treasuredata.com/v3/";
    public static final String GPDATA = "gpdata";
    public static final String GPDATA_ADMIN = "JpSrvWgGpdata";
    public static final String GPDATA_API_PREFIX = "https://gp-data.gree-office.net/v2/";
    public static final List<String> JOB_STATUS_SUCCESS = Arrays.asList(new String[]{"success", "FINISHED"});
    private final HttpClient httpClient;

    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    //private final Supplier<Map<String, Map<String, TdjobTable>>> schemas;
    private final ConcurrentMap<String, ConcurrentMap<String, TdjobTable>> schemas;
    private final Map<String, String> keyStore;

    @Inject
    public TdjobClient(TdjobConfig config, JsonCodec<TdjobListResult> jobsCodec)
            throws IOException
    {
        checkNotNull(config, "config is null");
        checkNotNull(jobsCodec, "jobsCodec is null");

        this.httpClient = new JettyHttpClient(
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(300, TimeUnit.SECONDS))
                //.setSocksProxy(HostAndPort.fromString("localhost:10080"))
        );
        keyStore = config.getMetadataKeys();

        schemas = schemasSupplier(httpClient, jobsCodec, keyStore).get();
    }

    public Set<String> getSchemaNames()
    {
        return schemas.keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        checkNotNull(schema, "schema is null");
        Map<String, TdjobTable> tables = schemas.get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public TdjobTable getTable(String schema, String tableName)
    {
        checkNotNull(schema, "schema is null");
        checkNotNull(tableName, "tableName is null");
        ConcurrentMap<String, TdjobTable> tables = schemas.get(schema);
        if (tables == null) {
            return null;
        }
        else if (tables.containsKey(tableName)) {
            return tables.get(tableName);
        }
        else {
            URI result;
            Request request;
            if (schema.equals(GPDATA)) {
                result = URI.create(GPDATA_API_PREFIX + "job/show/" + tableName);
                request = new Request(result, "GET",
                        ImmutableListMultimap.of("x-api-token", keyStore.get(schema), "x-user", GPDATA_ADMIN), null);
            }
            else {
                result = URI.create(TD_API_PREFIX + "job/show/" + tableName);
                request = new Request(result, "GET", ImmutableListMultimap.of("AUTHORIZATION", "TD1 " + keyStore.get(schema)), null);
            }

            FullJsonResponseHandler.JsonResponse<TdjobTable> execute = httpClient.execute(request,
                    FullJsonResponseHandler.createFullJsonResponseHandler(JsonCodec.jsonCodec(TdjobTable.class)));
            try {
                TdjobTable table = execute.getValue();
                if (JOB_STATUS_SUCCESS.contains(table.getStatus())) {
                    tables.put(tableName, table);
                    return table;
                }
            }
            catch (IllegalStateException e) {
            }
            return null;
        }
    }

    private static Supplier<ConcurrentMap<String, ConcurrentMap<String, TdjobTable>>> schemasSupplier(
            final HttpClient httpClient,
            final JsonCodec<TdjobListResult> jobsCodes,
            final Map<String, String> metadataKeys)
    {
        return new Supplier<ConcurrentMap<String, ConcurrentMap<String, TdjobTable>>>()
        {
            @Override
            public ConcurrentMap<String, ConcurrentMap<String, TdjobTable>> get()
            {
                try {
                    ConcurrentMap<String, ConcurrentMap<String, TdjobTable>> schemas = new MapMaker()
                            .makeMap();

                    //ImmutableMap.Builder<String, Map<String, TdjobTable>> builder = ImmutableMap.builder();
                    ConcurrentMap<String, TdjobTable> tmpDbJobs;
                    for (final String key : metadataKeys.keySet()) {
                        if (key.equals(GPDATA)) {
                            tmpDbJobs = new MapMaker().makeMap();
                        }
                        else {
                            tmpDbJobs = lookupSchemas(httpClient, metadataKeys.get(key), jobsCodes);
                        }
                        schemas.put(key, tmpDbJobs);
                    }
                    return schemas;
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    private static ConcurrentMap<String, TdjobTable> lookupSchemas(HttpClient httpClient, String key, JsonCodec<TdjobListResult> jobsCodec)
            throws IOException
    {
        URI result = URI.create(TD_API_PREFIX + "job/list?from=0&to=29");
        Request request = new Request(result, "GET", ImmutableListMultimap.of("AUTHORIZATION", "TD1 " + key), null);

        FullJsonResponseHandler.JsonResponse<TdjobListResult> execute = httpClient.execute(request, FullJsonResponseHandler.createFullJsonResponseHandler(jobsCodec));
        TdjobListResult jobs = execute.getValue();

        ConcurrentMap<String, TdjobTable> tables = new MapMaker()
                .makeMap();

        List<TdjobTable> filtered = Lists.newArrayList();
        for (TdjobTable job : jobs.getJobs()) {
            if (JOB_STATUS_SUCCESS.equals(job.getStatus())) {
                filtered.add(job);
            }
        }

        tables.putAll(Maps.uniqueIndex(filtered, nameGetter()));
        return tables;
    }

    public InputSupplier<InputStream> getJobResultSupplier(final String schema, final String tableName)
    {
        return new InputSupplier<InputStream>()
        {
            @Override
            public InputStream getInput()
            {
                URI result;
                Request request;
                if (schema.equals(GPDATA)) {
                    result = URI.create(GPDATA_API_PREFIX + "job/result/" + tableName + "?format=msgpack.gz");
                    request = new Request(result, "GET",
                            ImmutableListMultimap.of("x-api-token", keyStore.get(schema), "x-user", GPDATA_ADMIN), null);
                }
                else {
                    result = URI.create(TD_API_PREFIX + "job/result/" + tableName + "?format=msgpack.gz");
                    request = new Request(result, "GET", ImmutableListMultimap.of("AUTHORIZATION", "TD1 " + keyStore.get(schema)), null);
                }
                return httpClient.execute(request, RawStreamResponseHandler.createRawStreamResponseHandler());
            }
        };
    }

    public static class RawStreamResponseHandler implements ResponseHandler<InputStream, RuntimeException>
    {
        private static final MediaType MEDIA_TYPE_JSON = MediaType.create("application", "json");
        private static final MediaType MEDIA_TYPE_XGZIP = MediaType.create("application", "x-gzip");
        private static final RawStreamResponseHandler HANDLER = new RawStreamResponseHandler();

        public static RawStreamResponseHandler createRawStreamResponseHandler()
        {
            return HANDLER;
        }

        private RawStreamResponseHandler()
        {
        }

        @Override
        public InputStream handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public InputStream handle(Request request, Response response)
        {
            String contentType = response.getHeader(HttpHeaders.CONTENT_TYPE);
            if (!MediaType.parse(contentType).is(MEDIA_TYPE_XGZIP)) {
                throw new UnexpectedResponseException("Expected application/x-gzip response from server but got " + contentType, request, response);
            }

            try {
                return response.getInputStream();
            }
            catch (IOException e) {
                throw new RuntimeException("Error reading response from server");
            }
        }
    }
}
