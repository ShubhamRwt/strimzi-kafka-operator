/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.cluster.operator.assembly;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.KafkaRebalanceSpecBuilder;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlLoadParameters;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlRebalanceKeys;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.keycloak.util.JsonSerialization.mapper;


public class KafkaRebalanceStatusTest {

    private static final int BROKER_ONE_KEY = 1;

    private static final String BROKER_LOAD_KEY = "brokerLoad";
    private static final String RESOURCE_NAME = "my-rebalance";
    private static final String CLUSTER_NAMESPACE = "cruise-control-namespace";
    private static final String CLUSTER_NAME = "kafka-cruise-control-test-cluster";


    private KafkaRebalance createKafkaRebalance(String namespace, String clusterName, String resourceName,
                                                KafkaRebalanceSpec kafkaRebalanceSpec) {
        return new KafkaRebalanceBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(resourceName)
                .withLabels(clusterName != null ? Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME) : null)
                .endMetadata()
                .withSpec(kafkaRebalanceSpec)
                .build();
    }

    public static JsonObject buildOptimizationProposal() {

        JsonObject proposal = new JsonObject();

        JsonObject summary = new JsonObject();

        JsonObject brokersBeforeObject = new JsonObject();
        JsonArray brokerLoadBeforeArray = new JsonArray();
        JsonObject brokerOneBefore = new JsonObject();
        brokerOneBefore.put(CruiseControlRebalanceKeys.BROKER_ID.getKey(), BROKER_ONE_KEY);
        brokerOneBefore.put(CruiseControlLoadParameters.CPU_PERCENTAGE.getCruiseControlKey(), 10.0);
        brokerOneBefore.put(CruiseControlLoadParameters.REPLICAS.getCruiseControlKey(), 10);
        brokerLoadBeforeArray.add(brokerOneBefore);
        brokersBeforeObject.put(CruiseControlRebalanceKeys.BROKERS.getKey(), brokerLoadBeforeArray);

        JsonObject brokersAfterObject = new JsonObject();
        JsonArray brokerLoadAfterArray = new JsonArray();
        JsonObject brokerOneAfter = new JsonObject();
        brokerOneAfter.put(CruiseControlRebalanceKeys.BROKER_ID.getKey(), BROKER_ONE_KEY);
        brokerOneAfter.put(CruiseControlLoadParameters.CPU_PERCENTAGE.getCruiseControlKey(), 20.0);
        brokerOneAfter.put(CruiseControlLoadParameters.REPLICAS.getCruiseControlKey(), 5);
        brokerLoadAfterArray.add(brokerOneAfter);
        brokersAfterObject.put(CruiseControlRebalanceKeys.BROKERS.getKey(), brokerLoadAfterArray);

        proposal.put(CruiseControlRebalanceKeys.SUMMARY.getKey(), summary);
        proposal.put(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey(), brokersBeforeObject);
        proposal.put(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey(), brokersAfterObject);

        return proposal;

    }

    @Test
    public void testLoadParamExtract() {

        JsonObject proposal = buildOptimizationProposal();

        JsonArray loadBeforeArray = proposal.getJsonObject(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey())
                .getJsonArray(CruiseControlRebalanceKeys.BROKERS.getKey());

        Map<Integer, Map<String, Object>> output = KafkaRebalanceAssemblyOperator.extractLoadParameters(loadBeforeArray);

        assertThat(output, hasKey(BROKER_ONE_KEY));
        assertThat(output.get(BROKER_ONE_KEY), hasEntry(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey(), 10.0));
        assertThat(output.get(BROKER_ONE_KEY), hasEntry(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey(), 10));

    }

    @Test
    public void testCreateLoadMap() {

        JsonObject proposal = buildOptimizationProposal();

        JsonArray loadBeforeArray = proposal.getJsonObject(CruiseControlRebalanceKeys.LOAD_BEFORE_OPTIMIZATION.getKey())
                .getJsonArray(CruiseControlRebalanceKeys.BROKERS.getKey());
        JsonArray loadAfterArray = proposal.getJsonObject(CruiseControlRebalanceKeys.LOAD_AFTER_OPTIMIZATION.getKey())
                .getJsonArray(CruiseControlRebalanceKeys.BROKERS.getKey());

        JsonObject output = KafkaRebalanceAssemblyOperator.parseLoadStats(
                loadBeforeArray, loadAfterArray);

        assertThat(output.getMap(), hasKey("1"));

        assertThat(output.getJsonObject("1").getMap(), hasKey(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey()));

        JsonObject replicas = output.getJsonObject("1").getJsonObject("replicas");

        ArrayList<Integer> replicasList = new ArrayList<>();
        replicasList.add(replicas.getInteger("before"));
        replicasList.add(replicas.getInteger("after"));
        replicasList.add(replicas.getInteger("diff"));

        assertThat(replicasList.get(0), is(10));
        assertThat(replicasList.get(1), is(5));
        assertThat(replicasList.get(2), is(-5));

        assertThat(output.getJsonObject("1").getMap(), hasKey(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey()));

        JsonObject cpus = output.getJsonObject("1").getJsonObject("cpuPercentage");

        ArrayList<Double> cpu = new ArrayList<>();

        cpu.add(cpus.getDouble("before"));
        cpu.add(cpus.getDouble("after"));
        cpu.add(cpus.getDouble("diff"));

        assertThat(cpu.get(0), is(10.));
        assertThat(cpu.get(1), is(20.0));
        assertThat(cpu.get(2), is(10.0));

    }

    @Test
    public void testProcessProposal() {

        JsonObject proposal = buildOptimizationProposal();

        KafkaRebalance kr =
                createKafkaRebalance(CLUSTER_NAMESPACE, CLUSTER_NAME, RESOURCE_NAME, new KafkaRebalanceSpecBuilder().build());

        KafkaRebalanceAssemblyOperator.MapAndStatus<ConfigMap, Map<String, Object>> output = KafkaRebalanceAssemblyOperator.processOptimizationProposal(kr, proposal);

        assertTrue(output.getStatus().containsKey(CruiseControlRebalanceKeys.SUMMARY.getKey()));

        Map<String, String> brokerMap = output.getLoadMap().getData();

        try {

            ObjectMapper mapper = new ObjectMapper();

            Map<String, LinkedHashMap<String, String>> brokerLoadMap = mapper.readValue(brokerMap.get("brokerLoad"), LinkedHashMap.class);

            assertThat(brokerMap, hasKey(BROKER_LOAD_KEY));

            LinkedHashMap<String, LinkedHashMap<String, Object>> m = (LinkedHashMap) brokerLoadMap.get("1");

            assertThat(m, hasKey(CruiseControlLoadParameters.CPU_PERCENTAGE.getKafkaRebalanceStatusKey()));

            ArrayList<Double> cpu = new ArrayList<>();
            cpu.add((Double) m.get("cpuPercentage").get("before"));
            cpu.add((Double) m.get("cpuPercentage").get("after"));
            cpu.add((Double) m.get("cpuPercentage").get("diff"));

            assertThat(cpu.get(0), is(10.0));
            assertThat(cpu.get(1), is(20.0));
            assertThat(cpu.get(2), is(10.0));


            assertThat(m, hasKey(CruiseControlLoadParameters.REPLICAS.getKafkaRebalanceStatusKey()));

            ArrayList<Integer> replicas = new ArrayList<>();
            replicas.add((Integer) m.get("replicas").get("before"));
            replicas.add((Integer) m.get("replicas").get("after"));
            replicas.add((Integer) m.get("replicas").get("diff"));

            assertThat(replicas.get(0), is(10));
            assertThat(replicas.get(1), is(5));
            assertThat(replicas.get(2), is(-5));

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
