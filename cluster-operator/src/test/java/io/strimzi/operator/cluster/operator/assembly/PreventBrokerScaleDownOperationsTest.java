/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;

import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class PreventBrokerScaleDownOperationsTest {

    private static final String NAMESPACE = "my-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final Reconciliation RECONCILIATION = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, CLUSTER_NAME);
    private static Vertx vertx;

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    private final Set<TopicListing> topicListing = new HashSet<>();
    private final List<TopicDescription> topicDescriptions = new ArrayList<>();


    private void mockDescribeTopics(Admin mockAc) {
        when(mockAc.describeTopics(any(Collection.class))).thenAnswer(invocation -> {
            DescribeTopicsResult dtr = mock(DescribeTopicsResult.class);

            Map<String, TopicDescription> tds = Map.of("my-topic", topicDescriptions.get(0));

            when(dtr.allTopicNames()).thenReturn(KafkaFuture.completedFuture(tds));
            when(dtr.topicNameValues()).thenThrow(notImplemented());

            return dtr;
        });
    }

    private void addTopic(String topicName, Node leader) {
        addTopic(topicName, leader, List.of(leader), List.of(leader));
    }

    private Throwable notImplemented() {
        UnsupportedOperationException unsupportedOperationException = new UnsupportedOperationException("Not implemented by ");
        //unsupportedOperationException.printStackTrace();
        return unsupportedOperationException;
    }

    private void addTopic(String topicName, Node leader, List<Node> replicas, List<Node> isr) {
        Uuid topicId = Uuid.randomUuid();
        topicListing.add(new TopicListing(topicName, topicId, false));
        topicDescriptions.add(new TopicDescription(topicName, false,
                List.of(new TopicPartitionInfo(4,
                        leader, replicas, isr))));
    }

    private ListTopicsResult mockListTopics() {
        ListTopicsResult ltr = mock(ListTopicsResult.class);
        when(ltr.names()).thenAnswer(invocation -> KafkaFuture.completedFuture(Set.of("my-topic"))
        );
        when(ltr.listings()).thenThrow(notImplemented());
        when(ltr.namesToListings()).thenThrow(notImplemented());
        return ltr;
    }

    @Test
    public void testPartitionReplicasNotPresentOnRemovedBrokers(VertxTestContext context) {

        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);

        Node node = new Node(0, Node.noNode().host(), Node.noNode().port());
        addTopic("my-topic", node);

        ListTopicsResult ltr = mockListTopics();
        when(admin.listTopics(any())).thenReturn(ltr);

        mockDescribeTopics(admin);

        KafkaCluster cluster = mock(KafkaCluster.class);
        doReturn(Set.of(2, 3)).when(cluster).removedNodes();

        Checkpoint checkpoint = context.checkpoint();
        SecretOperator mockSecretOps = ResourceUtils.supplierWithMocks(false).secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        PreventBrokerScaleDownOperations operations = new PreventBrokerScaleDownOperations();

        operations.canScaleDownBrokers(vertx, RECONCILIATION, cluster.removedNodes(), mockSecretOps, mock)
                .onComplete(context.succeeding(s -> {
                    // the resource moved from ProposalPending to Stopped
                    assertThat(s.isEmpty(), is(true));
                    assertThat(s.size(), is(0));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testPartitionReplicasPresentOnBrokers(VertxTestContext context) {

        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);

        Node node = new Node(3, Node.noNode().host(), Node.noNode().port());
        addTopic("my-topic", node);

        ListTopicsResult ltr = mockListTopics();
        when(admin.listTopics(any())).thenReturn(ltr);

        mockDescribeTopics(admin);

        KafkaCluster cluster = mock(KafkaCluster.class);
        doReturn(Set.of(2, 3)).when(cluster).removedNodes();

        Checkpoint checkpoint = context.checkpoint();
        SecretOperator mockSecretOps = ResourceUtils.supplierWithMocks(false).secretOperations;
        Secret secret = new Secret();
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));
        when(mockSecretOps.getAsync(eq(NAMESPACE), eq(KafkaResources.secretName(CLUSTER_NAME)))).thenReturn(Future.succeededFuture(secret));

        PreventBrokerScaleDownOperations operations = new PreventBrokerScaleDownOperations();

        operations.canScaleDownBrokers(vertx, RECONCILIATION, cluster.removedNodes(), mockSecretOps, mock)
                .onComplete(context.succeeding(s -> {
                    // the resource moved from ProposalPending to Stopped
                    assertThat(s.isEmpty(), is(false));
                    assertThat(s.size(), is(1));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testTopicNamesBeingRetrievedCorrectly(VertxTestContext context) {

        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);

        Node node = new Node(3, Node.noNode().host(), Node.noNode().port());
        addTopic("my-topic", node);

        mockDescribeTopics(admin);

        ListTopicsResult ltr = mockListTopics();
        when(admin.listTopics(any())).thenReturn(ltr);

        Checkpoint checkpoint = context.checkpoint();

        PreventBrokerScaleDownOperations operations = new PreventBrokerScaleDownOperations();

        operations.topicNames(RECONCILIATION, admin)
                .onComplete(context.succeeding(topicNames -> {
                    assertThat(topicNames.size(), is(1));
                    assertEquals(topicNames, Set.of("my-topic"));
                    checkpoint.flag();
                }));
    }

    @Test
    public void testTopicDescriptionBeingRetrievedCorrectly(VertxTestContext context) {

        Admin admin = mock(Admin.class);
        AdminClientProvider mock = mock(AdminClientProvider.class);
        when(mock.createAdminClient(anyString(), any(), any(), anyString())).thenReturn(admin);

        Node node = new Node(3, Node.noNode().host(), Node.noNode().port());
        addTopic("my-topic", node);

        mockDescribeTopics(admin);

        ListTopicsResult ltr = mockListTopics();
        when(admin.listTopics(any())).thenReturn(ltr);

        Checkpoint checkpoint = context.checkpoint();

        PreventBrokerScaleDownOperations operations = new PreventBrokerScaleDownOperations();

        operations.describeTopics(RECONCILIATION, admin, Set.of("my-topic"))
                .onComplete(context.succeeding(topicDescriptions -> {
                    assertThat(topicDescriptions.size(), is(1));
                    checkpoint.flag();
                }));
    }
}

