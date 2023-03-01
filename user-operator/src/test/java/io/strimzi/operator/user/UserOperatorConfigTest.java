/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.model.Labels;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UserOperatorConfigTest {
    private static final Map<String, String> ENV_VARS = new HashMap<>(8);
    private static final Labels EXPECTED_LABELS;

    static {
        ENV_VARS.put(UserOperatorConfig.STRIMZI_NAMESPACE, "namespace");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "30000");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_LABELS, "label1=value1,label2=value2");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME, "ca-secret-cert");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_CA_KEY_SECRET_NAME, "ca-secret-key");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_CA_NAMESPACE, "differentnamespace");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_CLIENTS_CA_VALIDITY, "1000");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_CLIENTS_CA_RENEWAL, "10");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_ACLS_ADMIN_API_SUPPORTED, "false");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_KRAFT_ENABLED, "true");
        ENV_VARS.put(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, "20");


        Map<String, String> labels = new HashMap<>(2);
        labels.put("label1", "value1");
        labels.put("label2", "value2");

        EXPECTED_LABELS = Labels.fromMap(labels);
    }

    @Test
    public void testFromMap()    {
        UserOperatorConfig config = UserOperatorConfig.buildFromMap(ENV_VARS);

        assertThat(config.getNamespace(), is(ENV_VARS.get(UserOperatorConfig.STRIMZI_NAMESPACE)));
        assertThat(config.getReconciliationIntervalMs(), is(Long.parseLong(ENV_VARS.get(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS))));
        assertThat(config.getLabels(), is(EXPECTED_LABELS));
        assertThat(config.getCaCertSecretName(), is(ENV_VARS.get(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME)));
        assertThat(config.getCaNamespace(), is(ENV_VARS.get(UserOperatorConfig.STRIMZI_CA_NAMESPACE)));
        assertThat(config.getClientsCaValidityDays(), is(1000));
        assertThat(config.getClientsCaRenewalDays(), is(10));
        assertThat(config.isAclsAdminApiSupported(), is(false));
        assertThat(config.isKraftEnabled(), is(true));
        assertThat(config.getScramPasswordLength(), is(20));
        assertThat(config.getMaintenanceWindows(), is(nullValue()));
        assertThat(config.getOperationTimeoutMs(), is(300_000L));
        assertThat(config.getWorkQueueSize(), is(1_024));
        assertThat(config.getControllerThreadPoolSize(), is(50));
        assertThat(config.getCacheRefresh(), is(15_000L));
        assertThat(config.getBatchQueueSize(), is(1_024));
        assertThat(config.getBatchMaxBlockSize(), is(100));
        assertThat(config.getBatchMaxBlockTime(), is(100));
        assertThat(config.getUserOperationsThreadPoolSize(), is(4));
    }

    @Test
    public void testFromMapNamespaceEnvVarMissingThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_NAMESPACE);

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapCaNameEnvVarMissingThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_CA_CERT_SECRET_NAME);

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapReconciliationIntervalEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS);

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.getReconciliationIntervalMs(), is(Long.parseLong(UserOperatorConfig.RECONCILIATION_INTERVAL_MS.defaultValue())));
    }

    @Test
    public void testFromMapScramPasswordLengthEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH);

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH), is(Integer.parseInt(UserOperatorConfig.SCRAM_SHA_PASSWORD_LENGTH.defaultValue())));
    }

    @Test
    public void testFromMapStrimziLabelsEnvVarMissingSetsEmptyLabels()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_LABELS);

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.LABELS), is(Labels.EMPTY));
    }

    @Test
    public void testFromMapCaNamespaceMissingUsesNamespaceInstead()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_CA_NAMESPACE);

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.CA_NAMESPACE), is(envVars.get(UserOperatorConfig.STRIMZI_NAMESPACE)));
    }

    @Test
    public void testFromMapInvalidReconciliationIntervalThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, "not_a_long");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapInvalidScramPasswordLengthThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.STRIMZI_SCRAM_SHA_PASSWORD_LENGTH, "not_an_integer");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapInvalidLabelsStringThrows()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.STRIMZI_LABELS, ",label1=");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testFromMapCaValidityRenewalEnvVarMissingSetsDefault()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_CLIENTS_CA_VALIDITY);
        envVars.remove(UserOperatorConfig.STRIMZI_CLIENTS_CA_RENEWAL);

        UserOperatorConfig config =  UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.CERTS_VALIDITY_DAYS), is(Integer.parseInt(UserOperatorConfig.CERTS_VALIDITY_DAYS.defaultValue())));
        assertThat(config.get(UserOperatorConfig.CERTS_RENEWAL_DAYS), is(Integer.parseInt(UserOperatorConfig.CERTS_RENEWAL_DAYS.defaultValue())));
    }

    @Test
    public void testFromMapAclsAdminApiSupportedDefaults()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_ACLS_ADMIN_API_SUPPORTED);

        UserOperatorConfig config =  UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.ACLS_ADMIN_API_SUPPORTED), is(Boolean.parseBoolean(UserOperatorConfig.ACLS_ADMIN_API_SUPPORTED.defaultValue())));
    }

    @Test
    public void testFromMapKraftEnabledDefaults()  {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.remove(UserOperatorConfig.STRIMZI_KRAFT_ENABLED);

        UserOperatorConfig config =  UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.KRAFT_ENABLED), is(Boolean.parseBoolean(UserOperatorConfig.KRAFT_ENABLED.defaultValue())));
    }

    @Test
    public void testMaintenanceTimeWindows()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);

        envVars.put(UserOperatorConfig.STRIMZI_MAINTENANCE_TIME_WINDOWS, "* * 8-10 * * ?;* * 14-15 * * ?");

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.getMaintenanceWindows(), is(List.of("* * 8-10 * * ?", "* * 14-15 * * ?")));
    }

    @Test
    public void testKafkaAdminClientConfiguration() {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);

        envVars.put(UserOperatorConfig.STRIMZI_KAFKA_ADMIN_CLIENT_CONFIGURATION, "default.api.timeout.ms=13000\n" +
                "request.timeout.ms=130000");

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);

        assertThat(config.getKafkaAdminClientConfiguration().get("default.api.timeout.ms"), is("13000"));
        assertThat(config.getKafkaAdminClientConfiguration().get("request.timeout.ms"), is("130000"));
    }

    @Test
    public void testParseNullKafkaAdminClientConfiguration() {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);

        envVars.put(UserOperatorConfig.STRIMZI_MAINTENANCE_TIME_WINDOWS, null);

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);

        assertThat(config.getKafkaAdminClientConfiguration().isEmpty(), is(true));
    }

    @Test
    public void testWorkQueueSize()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.STRIMZI_WORK_QUEUE_SIZE, "2048");

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.WORK_QUEUE_SIZE), is(2048));
    }

    @Test
    public void testInvalidWorkQueueSize()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.STRIMZI_WORK_QUEUE_SIZE, "abcdefg");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }

    @Test
    public void testOperationTimeout()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS, "120000");

        UserOperatorConfig config = UserOperatorConfig.buildFromMap(envVars);
        assertThat(config.get(UserOperatorConfig.OPERATION_TIMEOUT_MS), is(120000L));
    }

    @Test
    public void testInvalidOperationTimeout()    {
        Map<String, String> envVars = new HashMap<>(UserOperatorConfigTest.ENV_VARS);
        envVars.put(UserOperatorConfig.STRIMZI_OPERATION_TIMEOUT_MS, "abcdefg");

        assertThrows(InvalidConfigurationException.class, () -> UserOperatorConfig.buildFromMap(envVars));
    }
}
