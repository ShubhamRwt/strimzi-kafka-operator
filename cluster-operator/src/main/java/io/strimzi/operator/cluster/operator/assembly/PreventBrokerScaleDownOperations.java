/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
/**
 * Class which contains several utility function which check if broker scale down can be done or not.
 */
public class PreventBrokerScaleDownOperations {

    /* logger*/
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaReconciler.class.getName());

    /**
     * Checks if broker contains any partition replicas when scaling down
     *
     * @param vertx               Instance of vertx
     * @param reconciliation      Reconciliation marker
     * @param removedNodes        Set of removed node from the cluster
     * @param secretOperator      Secret operator for working with Secrets
     * @param adminClientProvider Used to create the Admin client instance
     * @return Future which completes when the check is complete
     */
    public Future<Set<Integer>> canScaleDownBrokers(Vertx vertx, Reconciliation reconciliation, Set<Integer> removedNodes,
                                                   SecretOperator secretOperator, AdminClientProvider adminClientProvider) {

        if (removedNodes.isEmpty()) {
            return Future.succeededFuture(Set.of());
        } else {
            return canScaleDownBrokerCheck(vertx, reconciliation, secretOperator, adminClientProvider, removedNodes);
        }
    }

    /**
     * Checks if broker contains any partition replicas when scaling down
     *
     * @param vertx                Instance of vertx
     * @param reconciliation       Reconciliation marker
     * @param secretOperator       Secret operator for working with Secrets
     * @param adminClientProvider  Used to create the Admin client instance
     * @param idsToBeRemoved       Ids to be removed

     * @return  returns a boolean future based on the outcome of the check
     */
    public Future<Set<Integer>> canScaleDownBrokerCheck(Vertx vertx, Reconciliation reconciliation,
                                                          SecretOperator secretOperator, AdminClientProvider adminClientProvider, Set<Integer> idsToBeRemoved) {

        Promise<Set<Integer>> cannotScaleDown = Promise.promise();
        ReconcilerUtils.clientSecrets(reconciliation, secretOperator)
                .compose(compositeFuture -> {
                    Promise<Void> resultPromise = Promise.promise();
                    vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                            future -> {
                                final Future<Collection<TopicDescription>> descriptions;
                                try {
                                    String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
                                    LOGGER.infoCr(reconciliation, "Creating AdminClient for cluster {}/{}", reconciliation.namespace());
                                    Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, compositeFuture.resultAt(0), compositeFuture.resultAt(1), "cluster-operator");

                                    Future<Set<String>> topicNames = topicNames(reconciliation, kafkaAdmin);

                                    descriptions = topicNames.compose(names -> {
                                        LOGGER.infoCr(reconciliation, "Topic names {}", names);
                                        return describeTopics(reconciliation, kafkaAdmin, names);
                                    });
                                    descriptions
                                            .compose(topicDescriptions -> {
                                                LOGGER.infoCr(reconciliation, "Got {} topic descriptions", topicDescriptions.size());
                                                Set<Integer> idContainingPartitionReplicas = new HashSet<>();
                                                // Provides the node IDs which the user would like to remove first
                                                for (Integer id: idsToBeRemoved)   {
                                                    brokerHasAnyReplicas(reconciliation, topicDescriptions, id, idContainingPartitionReplicas);
                                                }
                                                cannotScaleDown.complete(idContainingPartitionReplicas);
                                                return cannotScaleDown.future();
                                            }).recover(error -> {
                                                LOGGER.warnCr(reconciliation, "failed to get topic descriptions", error);
                                                cannotScaleDown.fail(error);
                                                return Future.failedFuture(error);
                                            });

                                } catch (KafkaException e) {
                                    LOGGER.warnCr(reconciliation, "Kafka exception getting clusterId {}", e.getMessage());
                                }
                                future.complete();
                            }, true, resultPromise);
                    return resultPromise.future();
                });

        return cannotScaleDown.future();
    }

    /**
     * This method gets the topic names after interacting with the Admin client
     *
     * @param reconciliation                Instance of vertx
     * @param kafkaAdmin                    Instance of Kafka Admin
     * @return  return the set of topic names
     */
    protected Future<Set<String>> topicNames(Reconciliation reconciliation, Admin kafkaAdmin) {
        Promise<Set<String>> namesPromise = Promise.promise();
        kafkaAdmin.listTopics(new ListTopicsOptions().listInternal(true)).names()
                .whenComplete((names, error) -> {
                    if (error != null) {
                        namesPromise.fail(error);
                    } else {
                        LOGGER.debugCr(reconciliation, "Got {} topic names", names.size());
                        namesPromise.complete(names);
                    }
                });
        return namesPromise.future();
    }

    /**
     * Returns a collection of topic descriptions
     *
     * @param reconciliation       Reconciliation marker
     * @param kafkaAdmin           Instance of Admin client
     * @param names                Set of topic names
     * @return  Future which completes when the check is complete
     */
    protected Future<Collection<TopicDescription>> describeTopics(Reconciliation reconciliation, Admin kafkaAdmin, Set<String> names) {
        Promise<Collection<TopicDescription>> descPromise = Promise.promise();
        kafkaAdmin.describeTopics(names).allTopicNames()
                .whenComplete((tds, error) -> {
                    if (error != null) {
                        descPromise.fail(error);
                    } else {
                        LOGGER.debugCr(reconciliation, "Got topic descriptions for {} topics", tds.size());
                        descPromise.complete(tds.values());
                    }
                });
        return descPromise.future();
    }

    /**
     * Checks if broker contains any partition replicas
     *
     * @param reconciliation                 Reconciliation marker
     * @param podId                          Id of broker
     * @param tds                            Collection of Topic description
     * @param idsContainingParitionReplicas  Set of broker ids containing partition replicas
     */
    private void brokerHasAnyReplicas(Reconciliation reconciliation, Collection<TopicDescription> tds, int podId, Set<Integer> idsContainingParitionReplicas) {

        for (TopicDescription td : tds) {
            LOGGER.traceCr(reconciliation, td);
            for (TopicPartitionInfo pd : td.partitions()) {
                for (org.apache.kafka.common.Node broker : pd.replicas()) {
                    if (podId == broker.id()) {
                        idsContainingParitionReplicas.add(podId);
                        return;
                    }
                }
            }
        }
    }
}
