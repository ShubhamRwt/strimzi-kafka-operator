/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.MockSharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.VolumeUtils;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceAccountOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.vertx.core.Future;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ZooKeeperEraserTest {

    private static final String NAMESPACE = "my-namespace";
    private static final String NAME = "my-cluster";
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();

    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                    .withReplicas(3)
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withPort(9092)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(false)
                            .build())
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endKafka()
                .withNewZookeeper()
                    .withReplicas(3)
                    .withNewPersistentClaimStorage()
                        .withSize("123")
                        .withStorageClass("foo")
                    .endPersistentClaimStorage()
                .endZookeeper()
            .endSpec()
            .build();

    @Test
    public void testZookeperEraserReconcile(VertxTestContext context) {

        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        ServiceAccountOperator mockSaOps = supplier.serviceAccountOperations;
        ServiceOperator mockServiceOps = supplier.serviceOperations;
        NetworkPolicyOperator mockNetPolicyOps = supplier.networkPolicyOperator;
        ConfigMapOperator mockCmOps = supplier.configMapOperations;
        StrimziPodSetOperator mockPodSetOps = supplier.strimziPodSetOperator;
        PodDisruptionBudgetOperator mockPdbOps = supplier.podDisruptionBudgetOperator;
        SecretOperator mockSecretOps = supplier.secretOperations;

        ZookeeperCluster zkCluster = ZookeeperCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, VERSIONS, SHARED_ENV_PROVIDER);

        StrimziPodSet zkPodSet = zkCluster.generatePodSet(KAFKA.getSpec().getZookeeper().getReplicas(), false, null, null, podNum -> null);

        ArgumentCaptor<StrimziPodSet> zkPodSetCaptor =  ArgumentCaptor.forClass(StrimziPodSet.class);
        when(mockPodSetOps.getAsync(any(), any())).thenReturn(Future.succeededFuture(zkPodSet));
        when(mockPodSetOps.reconcile(any(), any(), any(), zkPodSetCaptor.capture())).thenAnswer(i -> Future.succeededFuture(ReconcileResult.noop(i.getArgument(3))));

        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(any(), anyString(), any(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ServiceAccount> saCaptor = ArgumentCaptor.forClass(ServiceAccount.class);
        when(mockSaOps.reconcile(any(), anyString(), any(), saCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<Service> serviceCaptor = ArgumentCaptor.forClass(Service.class);
        when(mockServiceOps.reconcile(any(), anyString(), any(), serviceCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<NetworkPolicy> netPolicyCaptor = ArgumentCaptor.forClass(NetworkPolicy.class);
        when(mockNetPolicyOps.reconcile(any(), anyString(), any(), netPolicyCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<ConfigMap> cmCaptor = ArgumentCaptor.forClass(ConfigMap.class);
        when(mockCmOps.reconcile(any(), anyString(), any(), cmCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<PodDisruptionBudget> pdbCaptor = ArgumentCaptor.forClass(PodDisruptionBudget.class);
        when(mockPdbOps.reconcile(any(), anyString(), any(), pdbCaptor.capture())).thenReturn(Future.succeededFuture());

        // Mock the PVC Operator
        PvcOperator mockPvcOps = supplier.pvcOperations;

        Map<String, PersistentVolumeClaim> zkPvcs = createZooPvcs(NAMESPACE, zkCluster.getStorage(), zkCluster.nodes(),
                (replica, storageId) -> VolumeUtils.DATA_VOLUME_NAME + "-" + KafkaResources.zookeeperPodName(KAFKA.getMetadata().getName(), replica));

        when(mockPvcOps.get(anyString(), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(zkCluster.getComponentName())) {
                        return zkPvcs.get(pvcName);
                    }
                    return null;
                });

        when(mockPvcOps.getAsync(anyString(), ArgumentMatchers.startsWith("data-")))
                .thenAnswer(invocation -> {
                    String pvcName = invocation.getArgument(1);
                    if (pvcName.contains(zkCluster.getComponentName())) {
                        return Future.succeededFuture(zkPvcs.get(pvcName));
                    }
                    return Future.succeededFuture(null);
                });

        when(mockPvcOps.listAsync(anyString(), ArgumentMatchers.any(Labels.class)))
                .thenAnswer(invocation -> Future.succeededFuture(zkPvcs.values().stream().toList()));

        ArgumentCaptor<PersistentVolumeClaim> pvcCaptor = ArgumentCaptor.forClass(PersistentVolumeClaim.class);
        when(mockPvcOps.reconcile(any(), anyString(), anyString(), pvcCaptor.capture())).thenReturn(Future.succeededFuture());

        // test reconcile
        ZooKeeperEraser rcnclr = new ZooKeeperEraser(
                Reconciliation.DUMMY_RECONCILIATION,
                ResourceUtils.dummyClusterOperatorConfig(),
                supplier,
                KAFKA
        );

        Checkpoint async = context.checkpoint();
        rcnclr.reconcile()
                .onComplete(context.succeeding(v -> context.verify(() -> {
                    assertThat(saCaptor.getAllValues().size(), is(1));
                    assertThat(saCaptor.getValue(), is(nullValue()));

                    assertThat(netPolicyCaptor.getAllValues().size(), is(1));
                    assertThat(netPolicyCaptor.getValue(), is(nullValue()));

                    assertThat(zkPodSetCaptor.getAllValues().size(), is(1));
                    assertThat(zkPodSetCaptor.getValue(), is(nullValue()));

                    assertThat(serviceCaptor.getAllValues().size(), is(2));
                    assertThat(serviceCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(serviceCaptor.getAllValues().get(1), is(nullValue()));

                    assertThat(secretCaptor.getAllValues().size(), is(1));
                    assertThat(secretCaptor.getValue(), is(nullValue()));

                    assertThat(cmCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(nullValue()));

                    assertThat(pdbCaptor.getAllValues().size(), is(1));
                    assertThat(pdbCaptor.getValue(), is(nullValue()));

                    // Check PVCs
                    assertThat(pvcCaptor.getAllValues().size(), is(3));
                    assertThat(pvcCaptor.getAllValues().get(0), is(nullValue()));
                    assertThat(pvcCaptor.getAllValues().get(1), is(nullValue()));
                    assertThat(pvcCaptor.getAllValues().get(2), is(nullValue()));

                    async.flag();
                })));
    }

    private Map<String, PersistentVolumeClaim> createZooPvcs(String namespace, Storage storage, Set<NodeRef> nodes,
                                                             BiFunction<Integer, Integer, String> pvcNameFunction) {

        Map<String, PersistentVolumeClaim> pvcs = new HashMap<>();
        for (NodeRef node : nodes) {
            Integer storageId = ((PersistentClaimStorage) storage).getId();
            String pvcName = pvcNameFunction.apply(node.nodeId(), storageId);
            pvcs.put(pvcName, createPvc(namespace, pvcName));
        }
        return pvcs;
    }

    private PersistentVolumeClaim createPvc(String namespace, String pvcName) {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(pvcName)
                .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, "true"))
                .endMetadata()
                .build();
    }


}
