/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.notifier;

import com.linkedin.kafka.cruisecontrol.detector.BrokerFailures;
import com.linkedin.kafka.cruisecontrol.detector.DiskFailures;
import com.linkedin.kafka.cruisecontrol.detector.GoalViolations;
import com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.MaintenanceEvent;
import com.linkedin.kafka.cruisecontrol.detector.TopicAnomaly;
import com.linkedin.kafka.cruisecontrol.detector.notifier.AnomalyNotificationResult;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.events.v1.EventBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Strimzi notifer class
 */


public class StrimziNotifier extends SelfHealingNotifier {

    protected static final String ACTION = "DetectedAnomalyGettingFixed";
    private static final DateTimeFormatter K8S_MICROTIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'.'SSSSSSXXX");



    @Override
    public AnomalyNotificationResult onGoalViolation(GoalViolations goalViolations) {
        return super.onGoalViolation(goalViolations);
    }

    @Override
    public AnomalyNotificationResult onMetricAnomaly(KafkaMetricAnomaly metricAnomaly) {
        return super.onMetricAnomaly(metricAnomaly);
    }

    @Override
    public AnomalyNotificationResult onTopicAnomaly(TopicAnomaly topicAnomaly) {
        System.out.println("TOpicAnomaly");
        return super.onTopicAnomaly(topicAnomaly);
    }

    @Override
    public AnomalyNotificationResult onMaintenanceEvent(MaintenanceEvent maintenanceEvent) {
        return super.onMaintenanceEvent(maintenanceEvent);
    }

    @Override
    public AnomalyNotificationResult onDiskFailure(DiskFailures diskFailures) {
        return super.onDiskFailure(diskFailures);
    }

    @Override
    public AnomalyNotificationResult onBrokerFailure(BrokerFailures brokerFailures) {
        System.out.println("BrokerFailure");
        return super.onBrokerFailure(brokerFailures);
    }

    @Override
    public void alert(com.linkedin.cruisecontrol.detector.Anomaly anomaly, boolean autoFixTriggered, long selfHealingStartTime, com.linkedin.cruisecontrol.detector.AnomalyType anomalyType) {
        super.alert(anomaly, autoFixTriggered, selfHealingStartTime, anomalyType);

        try (final KubernetesClient client = new KubernetesClientBuilder().build()) {

            MicroTime k8sEventTime = new MicroTime(K8S_MICROTIME.format(ZonedDateTime.now(Clock.systemDefaultZone())));
            ObjectReference podReference = createPodReference(client);
            String note = "Fixing the anomaly";
            String type = "Normal";

            System.out.println("Event Published " + anomaly.anomalyId());

            EventBuilder builder = new EventBuilder();

            builder.withNewMetadata()
                    .withName("my-cluster-" + anomaly.anomalyId())
                    .withGenerateName("cruise-control-event")
                    .endMetadata()
                    .withAction(ACTION)
                    .withReportingController("cruise-control")
                    .withReportingInstance("cruise-control")
                    .withRegarding(podReference)
                    .withReason("Anomaly was detected in the cluster " + anomaly.anomalyType())
                    .withType(type)
                    .withEventTime(k8sEventTime)
                    .withNote(note);

            client.events().v1().events().inNamespace(client.getNamespace()).resource(builder.build()).create();
        }
    }

    ObjectReference createPodReference(KubernetesClient client) {
        return new ObjectReferenceBuilder().withKind("Pod")
                .withNamespace(client.getNamespace())
                .withName("cruise-control-pod")
                .build();
    }

    @Override
    public void configure(Map<String, ?> config) {
        super.configure(config);
    }
}
