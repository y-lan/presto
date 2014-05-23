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
package com.facebook.presto.server;

import com.facebook.presto.event.query.QueryCompletionEvent;
import com.facebook.presto.event.query.QueryCreatedEvent;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.event.client.EventClient;
import io.airlift.event.client.JsonEventWriter;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryEventClient
        implements EventClient
{
    private static final Logger log = Logger.get(QueryEventClient.class);
    private final NodeInfo nodeInfo;
    private final JsonEventWriter eventWriter;
    private final JsonCodec<QueryCreatedEvent> queryCreatedEventJsonCodec;
    private final JsonCodec<QueryCompletionEvent> queryCompletionEventJsonCodec;

    @Inject
    public QueryEventClient(JsonEventWriter eventWriter, NodeInfo nodeInfo)
    {
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
        this.eventWriter = checkNotNull(eventWriter, "eventWriter is null");
        queryCreatedEventJsonCodec = JsonCodec.jsonCodec(QueryCreatedEvent.class);
        queryCompletionEventJsonCodec = JsonCodec.jsonCodec(QueryCompletionEvent.class);
    }

    @Override
    public <T> ListenableFuture<Void> post(T... event) throws IllegalArgumentException
    {
        Preconditions.checkNotNull(event, "event is null");
        return post(Arrays.asList(event));
    }

    @Override
    public <T> ListenableFuture<Void> post(final Iterable<T> events) throws IllegalArgumentException
    {
        checkNotNull(events, "eventsSupplier is null");
        return post(new EventGenerator<T>()
        {
            @Override
            public void generate(EventPoster<T> eventPoster)
                    throws IOException
            {
                for (T event : events) {
                    if (event instanceof QueryCreatedEvent || event instanceof QueryCompletionEvent) {
                        eventPoster.post(event);
                    }
                }
            }
        });
    }

    @Override
    public <T> ListenableFuture<Void> post(EventGenerator<T> eventGenerator) throws IllegalArgumentException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            eventWriter.writeEvents(eventGenerator, out);
            String s = out.toString("UTF-8");
            if (!s.equals("[]")) {
                log.info(s);
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return Futures.immediateFuture(null);
    }
}
