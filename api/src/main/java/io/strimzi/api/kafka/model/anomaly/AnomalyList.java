/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.anomaly;

import io.fabric8.kubernetes.api.model.DefaultKubernetesResourceList;

/**
 * A {@code DefaultKubernetesResourceList<KafkaBridge>} required for using Fabric8 CRD support.
 */
public class AnomalyList extends DefaultKubernetesResourceList<Anomaly> {
    private static final long serialVersionUID = 1L;
}
