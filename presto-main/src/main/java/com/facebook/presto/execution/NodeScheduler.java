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
package com.facebook.presto.execution;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.InetAddresses;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;

public class NodeScheduler
{
    private final NodeManager nodeManager;
    private final AtomicLong scheduleLocal = new AtomicLong();
    private final AtomicLong scheduleRack = new AtomicLong();
    private final AtomicLong scheduleRandom = new AtomicLong();
    private final int minCandidates;
    private final String rackTopologyScript;
    private final boolean locationAwareScheduling;

    @Inject
    public NodeScheduler(NodeManager nodeManager, NodeSchedulerConfig config)
    {
        this.nodeManager = nodeManager;
        this.minCandidates = config.getMinCandidates();
        this.rackTopologyScript = config.getRackTopologyScript();
        this.locationAwareScheduling = config.isLocationAwareSchedulingEnabled();
    }

    @Managed
    public long getScheduleLocal()
    {
        return scheduleLocal.get();
    }

    @Managed
    public long getScheduleRack()
    {
        return scheduleRack.get();
    }

    @Managed
    public long getScheduleRandom()
    {
        return scheduleRandom.get();
    }

    @Managed
    public void reset()
    {
        scheduleLocal.set(0);
        scheduleRack.set(0);
        scheduleRandom.set(0);
    }

    public NodeSelector createNodeSelector(final String dataSourceName, Map<Node, RemoteTask> taskMap, int maxPendingSplitsPerTask)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        Supplier<NodeMap> nodeMap = Suppliers.memoizeWithExpiration(new Supplier<NodeMap>()
        {
            @Override
            public NodeMap get()
            {
                ImmutableSetMultimap.Builder<HostAddress, Node> byHostAndPort = ImmutableSetMultimap.builder();
                ImmutableSetMultimap.Builder<InetAddress, Node> byHost = ImmutableSetMultimap.builder();
                ImmutableSetMultimap.Builder<Rack, Node> byRack = ImmutableSetMultimap.builder();

                Set<Node> nodes;
                if (dataSourceName != null) {
                    nodes = nodeManager.getActiveDatasourceNodes(dataSourceName);
                }
                else {
                    nodes = nodeManager.getActiveNodes();
                }

                for (Node node : nodes) {
                    try {
                        byHostAndPort.put(node.getHostAndPort(), node);

                        InetAddress host = InetAddress.getByName(node.getHttpUri().getHost());
                        byHost.put(host, node);

                        byRack.put(Rack.of(host, rackTopologyScript), node);
                    }
                    catch (UnknownHostException e) {
                        // ignore
                    }
                }

                return new NodeMap(byHostAndPort.build(), byHost.build(), byRack.build());
            }
        }, 5, TimeUnit.SECONDS);

        return new NodeSelector(nodeMap, taskMap, maxPendingSplitsPerTask);
    }

    public class NodeSelector
    {
        private final AtomicReference<Supplier<NodeMap>> nodeMap;
        private final Map<Node, RemoteTask> taskMap;
        private final int maxPendingSplitsPerTask;

        public NodeSelector(Supplier<NodeMap> nodeMap, Map<Node, RemoteTask> taskMap, int maxPendingSplitsPerTask)
        {
            this.nodeMap = new AtomicReference<>(nodeMap);
            this.taskMap = taskMap;
            this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        }

        public void lockDownNodes()
        {
            nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
        }

        public List<Node> allNodes()
        {
            return ImmutableList.copyOf(nodeMap.get().get().getNodesByHostAndPort().values());
        }

        public Node selectCurrentNode()
        {
            // TODO: this is a hack to force scheduling on the coordinator
            return nodeManager.getCurrentNode();
        }

        public List<Node> selectRandomNodes(int limit)
        {
            checkArgument(limit > 0, "limit must be at least 1");

            return ImmutableList.copyOf(FluentIterable.from(lazyShuffle(nodeMap.get().get().getNodesByHostAndPort().values())).limit(limit));
        }

        public Multimap<Node, Split> computeAssignments(Set<Split> splits)
        {
            Multimap<Node, Split> assignment = HashMultimap.create();

            for (Split split : splits) {
                List<Node> candidateNodes;
                if (locationAwareScheduling) {
                    candidateNodes = selectCandidateNodes(nodeMap.get().get(), split);
                }
                else {
                    candidateNodes = selectRandomNodes(minCandidates);
                }
                checkCondition(!candidateNodes.isEmpty(), NO_NODES_AVAILABLE, "No nodes available to run query");

                Node chosen = null;
                int min = Integer.MAX_VALUE;
                for (Node node : candidateNodes) {
                    RemoteTask task = taskMap.get(node);
                    int currentSplits = (task == null) ? 0 : task.getQueuedSplits();
                    int assignedSplits = currentSplits + assignment.get(node).size();
                    if (assignedSplits < min && assignedSplits < maxPendingSplitsPerTask) {
                        chosen = node;
                        min = assignedSplits;
                    }
                }
                if (chosen != null) {
                    assignment.put(chosen, split);
                }
            }
            return assignment;
        }

        private List<Node> selectCandidateNodes(NodeMap nodeMap, Split split)
        {
            Set<Node> chosen = new LinkedHashSet<>(minCandidates);

            // first look for nodes that match the hint
            for (HostAddress hint : split.getAddresses()) {
                for (Node node : nodeMap.getNodesByHostAndPort().get(hint)) {
                    if (chosen.add(node)) {
                        scheduleLocal.incrementAndGet();
                    }
                }

                InetAddress address;
                try {
                    address = hint.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip addresses that don't resolve
                    continue;
                }

                // consider a split with a host hint without a port as being accessible
                // by all nodes in that host
                if (!hint.hasPort() || split.isRemotelyAccessible()) {
                    for (Node node : nodeMap.getNodesByHost().get(address)) {
                        if (chosen.add(node)) {
                            scheduleLocal.incrementAndGet();
                        }
                    }
                }
            }

            // add nodes in same rack, if below the minimum count
            if (split.isRemotelyAccessible() && chosen.size() < minCandidates) {
                for (HostAddress hint : split.getAddresses()) {
                    InetAddress address;
                    try {
                        address = hint.toInetAddress();
                    }
                    catch (UnknownHostException e) {
                        // skip addresses that don't resolve
                        continue;
                    }
                    for (Node node : nodeMap.getNodesByRack().get(Rack.of(address, rackTopologyScript))) {
                        if (chosen.add(node)) {
                            scheduleRack.incrementAndGet();
                        }
                        if (chosen.size() == minCandidates) {
                            break;
                        }
                    }
                    if (chosen.size() == minCandidates) {
                        break;
                    }
                }
            }

            // add some random nodes if below the minimum count
            if (split.isRemotelyAccessible()) {
                if (chosen.size() < minCandidates) {
                    for (Node node : lazyShuffle(nodeMap.getNodesByHost().values())) {
                        if (chosen.add(node)) {
                            scheduleRandom.incrementAndGet();
                        }

                        if (chosen.size() == minCandidates) {
                            break;
                        }
                    }
                }
            }

            return ImmutableList.copyOf(chosen);
        }
    }

    private static <T> Iterable<T> lazyShuffle(final Iterable<T> iterable)
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return new AbstractIterator<T>()
                {
                    List<T> list = Lists.newArrayList(iterable);
                    int limit = list.size();

                    @Override
                    protected T computeNext()
                    {
                        if (limit == 0) {
                            return endOfData();
                        }

                        int position = ThreadLocalRandom.current().nextInt(limit);

                        T result = list.get(position);
                        list.set(position, list.get(limit - 1));
                        limit--;

                        return result;
                    }
                };
            }
        };
    }

    private static class NodeMap
    {
        private final SetMultimap<HostAddress, Node> nodesByHostAndPort;
        private final SetMultimap<InetAddress, Node> nodesByHost;
        private final SetMultimap<Rack, Node> nodesByRack;

        public NodeMap(SetMultimap<HostAddress, Node> nodesByHostAndPort, SetMultimap<InetAddress, Node> nodesByHost, SetMultimap<Rack, Node> nodesByRack)
        {
            this.nodesByHostAndPort = nodesByHostAndPort;
            this.nodesByHost = nodesByHost;
            this.nodesByRack = nodesByRack;
        }

        private SetMultimap<HostAddress, Node> getNodesByHostAndPort()
        {
            return nodesByHostAndPort;
        }

        public SetMultimap<InetAddress, Node> getNodesByHost()
        {
            return nodesByHost;
        }

        public SetMultimap<Rack, Node> getNodesByRack()
        {
            return nodesByRack;
        }
    }

    private static class Rack
    {
        public static final String DEFAULT_RACK = "/default/rack";
        private static ConcurrentHashMap<InetAddress, String> cache;
        private String id;

        public static Rack of(InetAddress address)
        {
            return Rack.of(address, null);
        }

        public static Rack of(InetAddress address, String rackTopologyScript)
        {
            if (cache == null) {
                cache = new ConcurrentHashMap<InetAddress, String>();
            }

            if (cache.containsKey(address)) {
                return new Rack(cache.get(address));
            }
            else {
                String id;
                if (rackTopologyScript != null) {
                    id = resolveByScript(address.getHostAddress(), rackTopologyScript);
                }
                else {
                    id = Integer.toString(InetAddresses.coerceToInteger(address) & 0xFF_FF_FF_00);
                }
                if (id != null && !id.isEmpty()) {
                    cache.put(address, id);
                    return new Rack(id);
                }
                else {
                    return new Rack(DEFAULT_RACK);
                }
            }
        }

        private static String resolveByScript(String address, String rackTopologyScript)
        {
            ProcessBuilder processBuilder = new ProcessBuilder(rackTopologyScript, address);
            Process process = null;
            try {
                process = processBuilder.start();
            }
            catch (IOException e) {
                return Rack.DEFAULT_RACK;
            }

            String line;
            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream())))
            {
                while ((line = br.readLine()) != null) {
                    sb.append(line).append(" ");
                }
            } catch (IOException e) {
                return Rack.DEFAULT_RACK;
            }

            return sb.toString().trim().split("\\s+")[0];
        }

        private Rack(String id)
        {
            this.id = id;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Rack rack = (Rack) o;

            if (!id.equals(rack.id)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return id.hashCode();
        }
    }
}
