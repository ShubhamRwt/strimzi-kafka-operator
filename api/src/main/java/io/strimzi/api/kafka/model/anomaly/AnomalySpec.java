/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.anomaly;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.Spec;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"anomalyId", "operation", "anomalyType"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class AnomalySpec extends Spec {

    private String anomalyId;
    private String operation;
    private String anomalyType;

    @Description("The id of the anomaly")
    public String getAnomalyId() {
        return anomalyId;
    }

    public void setAnomalyId(String anomalyId) {
        this.anomalyId = anomalyId;
    }

    @Description("The operation to be performed")
    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    @Description("Type of Anomaly")
    public String getAnomalyType() {
        return anomalyType;
    }

    public void setAnomalyType(String anomalyType) {
        this.anomalyType = anomalyType;
    }

}
