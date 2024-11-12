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
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Strimzi notifer class
 */
public class StrimziNotifier extends SelfHealingNotifier {

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

            System.out.println("Hello");
            Map<String, Object> spec = new HashMap<>();

            spec.put("anomalyId", anomaly.anomalyId());
            spec.put("operation", "");
            spec.put("anomalyType", anomaly.anomalyType());

            GenericKubernetesResource genericKubernetesResource = new GenericKubernetesResourceBuilder()
                    .withApiVersion("kafka.strimzi.io/v1beta2")
                    .withKind("Anomaly")
                    .withNewMetadata()
                        .withName("my-cluster-anomaly")
                    .endMetadata()
                    .addToAdditionalProperties("spec", spec)
                    .build();

            ResourceDefinitionContext context = new ResourceDefinitionContext.Builder()
                    .withGroup("kafka.strimzi.io")
                    .withVersion("v1beta2")
                    .withKind("Anomaly")
                    .withPlural("anomalies")
                    .withNamespaced(true)
                    .build();


            client.genericKubernetesResources(context).inNamespace(client.getNamespace()).resource(genericKubernetesResource).create();

        }
    }

    @Override
    public void configure(Map<String, ?> config) {
        super.configure(config);
    }
}
