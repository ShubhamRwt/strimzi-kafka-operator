/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.readiness.Readiness;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;

import java.util.Set;

/**
 *  Implementation of PlatformClient in terms of Kubernetes Pods
 */
public class PlatformClientImpl implements PlatformClient {

    private final PodOperator podOps;
    private final String namespace;

    private final Reconciliation reconciliation;

    private final KubernetesRestartEventPublisher eventPublisher;

    PlatformClientImpl(PodOperator podOps, String namespace, Reconciliation reconciliation, KubernetesRestartEventPublisher eventPublisher) {
        this.podOps = podOps;
        this.namespace = namespace;
        this.reconciliation = reconciliation;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public NodeState nodeState(NodeRef nodeRef) {
        var pod = podOps.get(namespace, nodeRef.podName());
        if (pod == null || pod.getStatus() == null) {
            return NodeState.NOT_RUNNING;
        } else {
            if (Readiness.isPodReady(pod)) {
                return NodeState.READY;
            } else {
                if (pendingAndUnschedulable(pod)) {
                    return NodeState.NOT_RUNNING; // NOT_RUNNING is more of a "likely stuck in not ready"
                } else if (hasWaitingContainerWithReason(pod, Set.of("CrashLoopBackoff", "ImagePullBackoff"))) {
                    return NodeState.NOT_RUNNING;
                }
                return NodeState.NOT_READY;
            }
        }

    }

    private static boolean hasWaitingContainerWithReason(Pod pod, Set<String> reasons) {
        return pod.getStatus().getContainerStatuses().stream().anyMatch(cs -> {
            if (cs.getState() != null && cs.getState().getWaiting() != null) {
                var waitingReason = cs.getState().getWaiting().getReason();
                return reasons.contains(waitingReason);
            } else {
                return false;
            }
        });
    }

    private static boolean pendingAndUnschedulable(Pod pod) {
        return "Pending".equals(pod.getStatus().getPhase()) && pod.getStatus().getConditions().stream().anyMatch(
                c -> "PodScheduled".equals(c.getType())
                        && "False".equals(c.getStatus())
                        && "Unschedulable".equals(c.getReason()));
    }

    @Override
    public void restartNode(NodeRef nodeRef, RestartReasons reason) {
        var pod = podOps.get(namespace, nodeRef.podName());
        podOps.restart(reconciliation, pod, 60_000)
                .onComplete(i ->
                    eventPublisher.publishRestartEvents(pod, reason)
                );
    }

    @Override
    public NodeRoles nodeRoles(NodeRef nodeRef) {
        Pod pod = podOps.get(namespace, nodeRef.podName());
        if (pod != null) {
            var podLabels = pod.getMetadata().getLabels();
            return new NodeRoles(Boolean.parseBoolean(podLabels.get(Labels.STRIMZI_CONTROLLER_ROLE_LABEL)),
                    Boolean.parseBoolean(podLabels.get(Labels.STRIMZI_BROKER_ROLE_LABEL)));
        } else {
            throw new RuntimeException("Could not find pod " + nodeRef.podName());
        }
    }
}
