/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.anomaly;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@JsonDeserialize
@Crd(
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = Anomaly.RESOURCE_KIND,
                        plural = Anomaly.RESOURCE_PLURAL,
                        shortNames = {Anomaly.SHORT_NAME},
                        categories = {Constants.STRIMZI_CATEGORY}
                ),
                group = KafkaRebalance.RESOURCE_GROUP,
                scope = KafkaRebalance.SCOPE,
                versions = {
                    @Crd.Spec.Version(name = Anomaly.V1BETA2, served = true, storage = false),
                    @Crd.Spec.Version(name = Anomaly.V1ALPHA1, served = true, storage = true)
                },
                subresources = @Crd.Spec.Subresources(
                    status = @Crd.Spec.Subresources.Status()
                )
        )
)
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API,
        refs = {@BuildableReference(CustomResource.class)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_GROUP_NAME)

public class Anomaly extends CustomResource<AnomalySpec, AnomalyStatus> implements Namespaced, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2, V1ALPHA1);
    public static final String RESOURCE_KIND = "Anomaly";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "anomalies";
    public static final String RESOURCE_SINGULAR = "anomaly";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "anomaly";
    public static final List<String> RESOURCE_SHORTNAMES = List.of(SHORT_NAME);

    private Map<String, Object> additionalProperties;

    // Added to avoid duplication during Json serialization
    private String apiVersion;
    private String kind;


    public Anomaly() {
        super();
    }

    public Anomaly(AnomalySpec spec, AnomalyStatus status) {
        super();
        this.spec = spec;
        this.status = status;
    }

    @Override
    @Description("The specification of the Kafka rebalance.")
    public AnomalySpec getSpec() {
        return super.getSpec();
    }

    @Override
    @Description("The status of the Kafka rebalance.")
    public AnomalyStatus getStatus() {
        return super.getStatus();
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return null;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {

    }
}
