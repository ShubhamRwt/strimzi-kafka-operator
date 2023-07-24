/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerConfigurationDiff;
import io.strimzi.operator.cluster.operator.resource.KafkaBrokerLoggingConfigurationDiff;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class RackRolling {

    private static boolean wouldBeUnderReplicated(Integer minIsr, Replica replica) {
        final boolean wouldByUnderReplicated;
        if (minIsr == null) {
            // if topic doesn't have minISR then it's fine
            wouldByUnderReplicated = false;
        } else {
            // else topic has minISR
            // compute spare = size(ISR) - minISR
            int sizeIsr = replica.isrSize();
            int spare = sizeIsr - minIsr;
            if (spare > 0) {
                // if (spare > 0) then we can restart the broker hosting this replica
                // without the topic being under-replicated
                wouldByUnderReplicated = false;
            } else if (spare == 0) {
                // if we restart this broker this replica would be under-replicated if it's currently in the ISR
                // if it's not in the ISR then restarting the server won't make a difference
                wouldByUnderReplicated = replica.isInIsr();
            } else {
                // this partition is already under-replicated
                // if it's not in the ISR then restarting the server won't make a difference
                // but in this case since it's already under-replicated let's
                // not possible prolong the time to this server rejoining the ISR
                wouldByUnderReplicated = true;
            }
        }
        return wouldByUnderReplicated;
    }

    private static boolean avail(Server server,
                                 Map<String, Integer> minIsrByTopic) {
        for (var replica : server.replicas()) {
            var topicName = replica.topicName();
            Integer minIsr = minIsrByTopic.get(topicName);
            if (wouldBeUnderReplicated(minIsr, replica)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Split the given cells into batches,
     * taking account of {@code acks=all} availability and the given maxBatchSize
     */
    static List<Set<Server>> batchCells(List<Set<Server>> cells,
                                               Map<String, Integer> minIsrByTopic,
                                               int maxBatchSize) {
        List<Set<Server>> result = new ArrayList<>();
        for (var cell : cells) {
            List<Set<Server>> availBatches = new ArrayList<>();
            Set<Server> unavail = new HashSet<>();
            for (var server : cell) {
                if (avail(server, minIsrByTopic)) {
                    var currentBatch = availBatches.isEmpty() ? null : availBatches.get(availBatches.size() - 1);
                    if (currentBatch == null || currentBatch.size() >= maxBatchSize) {
                        currentBatch = new HashSet<>();
                        availBatches.add(currentBatch);
                    }
                    currentBatch.add(server);
                } else {
                    unavail.add(server);
                }
            }
            result.addAll(availBatches);
        }
        return result;
    }

    static <T> boolean intersects(Set<T> set, Set<T> set2) {
        for (T t : set) {
            if (set2.contains(t)) {
                return true;
            }
        }
        return false;
    }

    static boolean containsAny(Set<Server> cell, Set<Replica> replicas) {
        for (var b : cell) {
            if (intersects(b.replicas(), replicas)) {
                return true;
            }
        }
        return false;
    }

    /** Returns a new set that is the union of each of the sets in the given {@code merge}. I.e. flatten without duplicates. */
    private static <T> Set<T> union(Set<Set<T>> merge) {
        HashSet<T> result = new HashSet<>();
        for (var x : merge) {
            result.addAll(x);
        }
        return result;
    }

    private static Set<Set<Server>> partitionByHasAnyReplicasInCommon(Set<Server> rollable) {
        Set<Set<Server>> disjoint = new HashSet<>();
        for (var server : rollable) {
            var replicas = server.replicas();
            Set<Set<Server>> merge = new HashSet<>();
            for (Set<Server> cell : disjoint) {
                if (!containsAny(cell, replicas)) {
                    merge.add(cell);
                    merge.add(Set.of(server));
                    // problem is here, we're iterating over all cells (ones which we've decided should be disjoint)
                    // and we merged them in violation of that
                    // we could break here at the end of the if block (which would be correct)
                    // but it might not be optimal (in the sense of forming large cells)
                    break;
                }
            }
            if (merge.isEmpty()) {
                disjoint.add(Set.of(server));
            } else {
                for (Set<Server> r : merge) {
                    disjoint.remove(r);
                }
                disjoint.add(union(merge));
            }
        }
        return disjoint;
    }

    /**
     * Partition the given {@code brokers}
     * into cells that can be rolled in parallel because they
     * contain no replicas in common.
     */
    static List<Set<Server>> cells(Collection<Server> brokers) {

        // find brokers that are individually rollable
        var rollable = brokers.stream().collect(Collectors.toCollection(() ->
                new TreeSet<>(Comparator.comparing(Server::id))));
        if (rollable.size() < 2) {
            return List.of(rollable);
        } else {
            // partition the set under the equivalence relation "shares a partition with"
            Set<Set<Server>> disjoint = partitionByHasAnyReplicasInCommon(rollable);
            // disjoint cannot be empty, because rollable isn't empty, and disjoint is a partitioning or rollable
            // We find the biggest set of brokers which can parallel-rolled
            var sorted = disjoint.stream().sorted(Comparator.<Set<?>>comparingInt(Set::size).reversed()).toList();
            return sorted;
        }
    }


    /**
     * Pick the "best" batch to be restarted.
     * This is the largest batch of available servers excluding the
     * @return the "best" batch to be restarted
     */
    static Set<Server> pickBestBatchForRestart(List<Set<Server>> batches, int controllerId) {
        var sorted = batches.stream().sorted(Comparator.comparing(Set::size)).toList();
        if (sorted.size() == 0) {
            return Set.of();
        }
        if (sorted.size() > 1
                && sorted.get(0).stream().anyMatch(s -> s.id() == controllerId)) {
            return sorted.get(1);
        }
        return sorted.get(0);
    }

    private static Set<Server> nextBatch(RollClient rollClient,
                                         Set<Integer> brokersNeedingRestart,
                                         int maxRestartBatchSize) throws ExecutionException, InterruptedException {

        Map<Integer, Server> servers = new HashMap<>();
        
        // TODO figure out this KRaft stuff
//        var quorum = admin.describeMetadataQuorum().quorumInfo().get();
//        var controllers = quorum.voters().stream()
//                .map(QuorumInfo.ReplicaState::replicaId)
//                .map(controllerId -> {
//                    return new Server(controllerId, null, Set.of(new Replica("::__cluster_metadata", 0, true)));
//                }).toList();

        // Get all the topics in the cluster
        Collection<TopicListing> topicListings = rollClient.listTopics();

        // batch the describeTopics requests to avoid trying to get the state of all topics in the cluster
        var topicIds = topicListings.stream().map(TopicListing::topicId).toList();

        // Convert the TopicDescriptions to the Server and Replicas model
        List<TopicDescription> topicDescriptions = rollClient.describeTopics(topicIds);

        topicDescriptions.forEach(topicDescription -> {
            topicDescription.partitions().forEach(partition -> {
                partition.replicas().forEach(replicatingBroker -> {
                    var server = servers.computeIfAbsent(replicatingBroker.id(), ig -> new Server(replicatingBroker.id(), replicatingBroker.rack(), new HashSet<>()));
                    server.replicas().add(new Replica(
                            replicatingBroker,
                            topicDescription.name(),
                            partition.partition(),
                            partition.isr()));
                });
            });
        });

        // Add any servers which we know about but which were absent from any partition metadata
        // i.e. brokers without any assigned partitions
        brokersNeedingRestart.forEach(server -> {
            servers.putIfAbsent(server, new Server(server, null, Set.of()));
        });

        // TODO somewhere in here we need to take account of partition reassignments
        //      e.g. if a partition is being reassigned we expect its ISR to change
        //      (see https://cwiki.apache.org/confluence/display/KAFKA/KIP-455%3A+Create+an+Administrative+API+for+Replica+Reassignment#KIP455:CreateanAdministrativeAPIforReplicaReassignment-Algorithm
        //      which guarantees that addingReplicas are honoured before removingReplicas)
        //      If there are any removingReplicas our availability calculation won't account for the fact
        //      that the controller may shrink the ISR during the reassignment.

        // Split the set of all brokers replicating any partition
        var cells = cells(servers.values());

        // filter each cell by brokers that actually need to be restarted
        cells = cells.stream().map(cell -> cell.stream().filter(server -> brokersNeedingRestart.contains(server.id())).collect(Collectors.toSet())).toList();

        var minIsrByTopic = rollClient.describeTopicMinIsrs(topicListings.stream().map(TopicListing::name).toList());
        var batches = batchCells(cells, minIsrByTopic, maxRestartBatchSize);

        int controllerId = rollClient.activeController();
        var bestBatch = pickBestBatchForRestart(batches, controllerId);
        return bestBatch;
    }

    public static void main(String[] a) {
        int numRacks = 3;
        int numBrokers = 10;
        int numControllers = 5;
        int numTopics = 1;
        int numPartitionsPerTopic = 10;
        boolean coloControllers = true;
        int rf = 3;
        System.out.printf("numRacks = %d%n", numRacks);
        System.out.printf("numBrokers = %d%n", numBrokers);
        System.out.printf("numTopics = %d%n", numTopics);
        System.out.printf("numPartitionsPerTopic = %d%n", numPartitionsPerTopic);
        System.out.printf("rf = %d%n", rf);

        List<Server> servers = new ArrayList<>();
        for (int serverId = 0; serverId < (coloControllers ? Math.max(numBrokers, numControllers) : numControllers + numBrokers); serverId++) {
            Server server = new Server(serverId, Integer.toString(serverId % numRacks), new LinkedHashSet<>());
            servers.add(server);
            boolean isController = serverId < numControllers;
            if (isController) {
                server.replicas().add(new Replica("__cluster_metadata", 0, (short) numControllers));
            }
        }

        for (int topic = 1; topic <= numTopics; topic++) {
            for (int partition = 0; partition < numPartitionsPerTopic; partition++) {
                for (int replica = partition; replica < partition + rf; replica++) {
                    Server broker = servers.get((coloControllers ? 0 : numControllers) + replica % numBrokers);
                    broker.replicas().add(new Replica("t" + topic, partition, (short) rf));
                }
            }
        }

        for (var broker : servers) {
            System.out.println(broker);
        }

        // TODO validate

        var results = cells(servers);

        int group = 0;
        for (var result : results) {
            System.out.println("Group " + group + ": " + result.stream().map(Server::id).collect(Collectors.toCollection(TreeSet::new)));
            group++;
        }
    }

    static String podName(NodeRef nodeRef) {
        return nodeRef.podName();
    }

    private static void restartServer(RollClient rollClient, Context context, int maxRestarts) {
        if (context.numRestarts() > maxRestarts) {
            throw new RuntimeException("Too many restarts"); // TODO proper exception type
        }
        rollClient.deletePod(context.nodeRef());
        context.transitionTo(State.RESTARTED);
        // TODO kube create an Event with the context.reason
    }

    private static void reconfigureServer(RollClient rollClient, Context context, int maxReconfigs) {
        if (context.numReconfigs() > maxReconfigs) {
            throw new RuntimeException("Too many reconfigs");
        }
        rollClient.reconfigureServer(context.nodeRef(), context.brokerConfigDiff(), context.loggingDiff());
        context.transitionTo(State.RECONFIGURED);
        // TODO create kube Event
    }


    private static long awaitState(Time time, RollClient rollClient, Context context, State targetState, long timeoutMs) throws InterruptedException, TimeoutException {
        return Alarm.timer(
                time,
                timeoutMs,
                () -> "Failed to reach " + targetState + " within " + timeoutMs + " ms: " + context
        ).poll(1_000, () -> {
            var state = context.transitionTo(rollClient.observe(context.nodeRef()));
            return state == targetState;
        });
    }

    private static long awaitPreferred(Time time, RollClient rollClient, Context context, long timeoutMs) throws InterruptedException, TimeoutException {
        return Alarm.timer(time,
                timeoutMs,
                () -> "Failed to reach " + State.LEADING_ALL_PREFERRED + " within " + timeoutMs + ": " + context)
        .poll(1_000, () -> {
            var remainingReplicas = rollClient.tryElectAllPreferredLeaders(context.nodeRef());
            if (remainingReplicas == 0) {
                context.transitionTo(State.LEADING_ALL_PREFERRED);
            }
            return remainingReplicas == 0;
        });
    }

    private static void restartInParallel(Time time, RollClient rollClient, Set<Context> batch, long timeoutMs, int maxRestarts) throws InterruptedException, TimeoutException {
        for (Context context : batch) {
            restartServer(rollClient, context, maxRestarts);
        }
        long remainingTimeoutMs = timeoutMs;
        for (Context context : batch) {
            remainingTimeoutMs = awaitState(time, rollClient, context, State.SERVING, remainingTimeoutMs);
        }

        var serverContextWrtIds = new HashMap<Integer, Context>();
        var nodeRefs = new ArrayList<NodeRef>();
        for (Context context : batch) {
            Integer id = context.serverId();
            nodeRefs.add(context.nodeRef());
            serverContextWrtIds.put(id, context);
        }

        Alarm.timer(time,
                remainingTimeoutMs,
                () -> "Servers " + nodeRefs + " failed to reach " + State.LEADING_ALL_PREFERRED + " within " + timeoutMs + ": " +
                        nodeRefs.stream().map(nodeRef -> serverContextWrtIds.get(nodeRef.nodeId())).collect(Collectors.toSet()))
            .poll(1_000, () -> {
                var toRemove = new ArrayList<NodeRef>();
                for (var nodeRef : nodeRefs) {
                    if (rollClient.tryElectAllPreferredLeaders(nodeRef) == 0) {
                        serverContextWrtIds.get(nodeRef.nodeId()).transitionTo(State.LEADING_ALL_PREFERRED);
                        toRemove.add(nodeRef);
                    }
                }
                nodeRefs.removeAll(toRemove);
                return nodeRefs.isEmpty();
            });
    }

    private static Map<Plan, List<Context>> refinePlanForReconfigurability(Reconciliation reconciliation,
                                                                           KafkaVersion kafkaVersion,
                                                                           Function<Integer, String> kafkaConfigProvider,
                                                                           String desiredLogging,
                                                                           RollClient rollClient,
                                                                           Map<Plan, List<Context>> byPlan) {
        var contexts = byPlan.getOrDefault(Plan.MAYBE_RECONFIGURE, List.of());
        var brokerConfigs = rollClient.describeBrokerConfigs(contexts.stream()
                .map(Context::nodeRef).toList());

        var xxx = contexts.stream().collect(Collectors.groupingBy(context -> {
            RollClient.Configs configPair = brokerConfigs.get(context.serverId());
            var diff = new KafkaBrokerConfigurationDiff(reconciliation,
                    configPair.brokerConfigs(),
                    kafkaConfigProvider.apply(context.serverId()),
                    kafkaVersion,
                    context.serverId());
            // TODO what is the source of truth about reconfiguration
            //      on the one hand we have the RestartReason, which might be a singleton of reconfig
            //      on the other hand there is the diff of current vs desired configs
            var loggingDiff = new KafkaBrokerLoggingConfigurationDiff(reconciliation, configPair.brokerLoggerConfigs(), desiredLogging);
            context.brokerConfigDiff(diff);
            context.loggingDiff(loggingDiff);
            if (!diff.isEmpty() && diff.canBeUpdatedDynamically()) {
                return Plan.RECONFIGURE;
            } else if (diff.isEmpty()) {
                return Plan.RECONFIGURE;
            } else {
                return Plan.RESTART;
            }
        }));

        return Map.of(
                Plan.RESTART, Stream.concat(byPlan.getOrDefault(Plan.RESTART, List.of()).stream(), xxx.getOrDefault(Plan.RESTART, List.of()).stream()).toList(),
                Plan.RECONFIGURE, xxx.getOrDefault(Plan.RECONFIGURE, List.of()),
                Plan.RESTART_FIRST, xxx.getOrDefault(Plan.RESTART_FIRST, List.of())
        );
    }

    enum Plan {
        // Used for brokers that are initially healthy and require neither restart not reconfigure
        NOP,
        // Used for brokers that are initially not healthy
        RESTART_FIRST,
        // Used in {@link #initialPlan(Function, List, int)} for brokers that require reconfigure
        // before we know whether the actual config changes are reconfigurable
        MAYBE_RECONFIGURE,
        // Used in {@link #refinePlanForReconfigurability(Reconciliation, KafkaVersion, Function, String, RollClient, Map)}
        // once we know a MAYBE_RECONFIGURE node can actually be reconfigured
        RECONFIGURE,
        RESTART
    }

    /**
     * Do a rolling restart (or reconfigure) of some of the Kafka servers given in {@code nodes}.
     * Servers that are not ready (in the Kubernetes sense) will always be considered for restart before any others.
     * The given {@code predicate} will be called for each of the remaining servers and those for which the function returns a non-empty
     * list of reasons will be restarted or reconfigured.
     * When a server is restarted this method guarantees to wait for it to enter the running broker state and
     * become the leader of all its preferred replicas.
     * If a server is not restarted by this method (because the {@code predicate} function returned empty), then
     * it may not be the leader of all its preferred replicas.
     * If this method completes normally then all initially unready servers and the servers for which the {@code predicate} function returned
     * a non-empty list of reasons (which may be no servers) will have been successfully restarted/reconfigured.
     * In other words, successful return from this method indicates that all servers seem to be up and
     * "functioning normally".
     * If a server fails to restart or recover its logs within a certain time this method will throw TimeoutException.
     *
     * The expected worst case execution time of this function is approximately
     * {@code (timeoutMs * maxRestarts + postReconfigureTimeoutMs) * size(nodes)}.
     * This is reached when:
     * <ol>
     *     <li>We initially attempt to reconfigure all nodes</li>
     *     <li>Those reconfigurations all fail, so we resort to restarts</li>
     *     <li>We require {@code maxRestarts} restarts for each node, and each restart uses the
     *         maximum {@code timeoutMs}.</li>
     * </ol>
     *
     * @param rollClient The roll client.
     * @param nodes The nodes.
     * @param predicate The predicate.
     * @param postReconfigureTimeoutMs The maximum time to wait after a reconfiguration.
     * @param postRestartTimeoutMs The maximum time to wait after a restart.
     * @param maxRestartBatchSize The maximum number of servers that might be restarted at once.
     * @param maxRestarts The maximum number of restarts attempted for any individual server
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public static void rollingRestart(Time time,
                                      RollClient rollClient,
                                      List<NodeRef> nodes,
                                      Function<Integer, RestartReasons> predicate,
                                      Reconciliation reconciliation,
                                      KafkaVersion kafkaVersion,
                                      Function<Integer, String> kafkaConfigProvider,
                                      String desiredLogging,
                                      long postReconfigureTimeoutMs,
                                      long postRestartTimeoutMs,
                                      int maxRestartBatchSize,
                                      int maxRestarts)
            throws InterruptedException, ExecutionException, TimeoutException {

        // Create contexts
        var contexts = nodes.stream().map(Context::start).toList();

        OUTER: while (true) {

            // Observe current state and update the contexts
            for (var context : contexts) {
                context.transitionTo(rollClient.observe(context.nodeRef()));
            }

            int maxReconfigs = 1;
            var byPlan = initialPlan(predicate, contexts, maxReconfigs);
            if (byPlan.getOrDefault(Plan.RESTART_FIRST, List.of()).isEmpty()
                    && byPlan.getOrDefault(Plan.RESTART, List.of()).isEmpty()
                    && byPlan.getOrDefault(Plan.MAYBE_RECONFIGURE, List.of()).isEmpty()) {
                // termination condition met
                break OUTER;
            }

            // Restart any initially unready nodes
            for (var context : byPlan.getOrDefault(Plan.RESTART_FIRST, List.of())) {
                context.reason(RestartReasons.of(RestartReason.POD_UNRESPONSIVE));
                restartServer(rollClient, context, maxRestarts);
                long remainingTimeoutMs = awaitState(time, rollClient, context, State.SERVING, postRestartTimeoutMs);
                awaitPreferred(time, rollClient, context, remainingTimeoutMs);
                continue OUTER;
            }
            // If we get this far we know all nodes are ready

            // Refine the plan, reassigning nodes under MAYBE_RECONFIGURE to either RECONFIGURE or RESTART
            // based on whether they have only reconfiguration config changes
            byPlan = refinePlanForReconfigurability(reconciliation,
                    kafkaVersion,
                    kafkaConfigProvider,
                    desiredLogging,
                    rollClient,
                    byPlan);

            // Reconfigure any reconfigurable nodes
            for (var context : byPlan.get(Plan.RECONFIGURE)) {
                // TODO decide on parallel/batching dynamic reconfiguration
                reconfigureServer(rollClient, context, maxReconfigs);
                time.sleep(postReconfigureTimeoutMs / 2, 0);
                awaitPreferred(time, rollClient, context, postReconfigureTimeoutMs / 2);
                // termination condition
                if (contexts.stream().allMatch(context2 -> context2.state() == State.LEADING_ALL_PREFERRED)) {
                    break OUTER;
                }
                continue OUTER;
            }
            // If we get this far that all remaining nodes require a restart

            // determine batches of nodes to be restarted together
            var batch = nextBatch(rollClient, byPlan.get(Plan.RESTART).stream().map(Context::serverId).collect(Collectors.toSet()), maxRestartBatchSize);
            var batchOfIds = batch.stream().map(Server::id).collect(Collectors.toSet());
            var batchOfContexts = contexts.stream().filter(context -> batchOfIds.contains(context.serverId())).collect(Collectors.toSet());
            // restart a batch
            restartInParallel(time, rollClient, batchOfContexts, postRestartTimeoutMs, maxRestarts);

            // termination condition
            if (contexts.stream().allMatch(context -> context.state() == State.LEADING_ALL_PREFERRED)) {
                break OUTER;
            }
        }
    }

    private static Map<Plan, List<Context>> initialPlan(Function<Integer, RestartReasons> predicate, List<Context> contexts, int maxReconfigs) {
        return contexts.stream().collect(Collectors.groupingBy(context -> {
            if (context.state() == State.NOT_READY) {
                return Plan.RESTART_FIRST;
            } else {
                var reasons = predicate.apply(context.serverId());
                // TODO the predicate is static: The reasons returned won't change even though we do the restart
                //      so we need a function f(reasons): plan  (where plan is sum of RESTART,RECONFIGURE,NOP
                //      and another g(plan, context): boolean   that uses info in the context to determine whether the plan worked
                if (reasons.getReasons().isEmpty()) {
                    return Plan.NOP;
                } else {
                    context.reason(reasons);
                    if (reasons.singletonOf(RestartReason.CONFIG_CHANGE_REQUIRES_RESTART)
                            && context.numReconfigs() < maxReconfigs) {
                        return Plan.MAYBE_RECONFIGURE;
                    } else {
                        return Plan.RESTART;
                    }
                }
            }
        }));
    }


}
