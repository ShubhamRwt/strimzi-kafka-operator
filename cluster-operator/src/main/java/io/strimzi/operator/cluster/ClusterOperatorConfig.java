/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.strimzi.operator.cluster.leaderelection.LeaderElectionManagerConfig;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NoImageException;
import io.strimzi.operator.cluster.model.UnsupportedVersionException;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.ConfigParameter;
import io.strimzi.operator.common.operator.resource.ConfigParameterParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static io.strimzi.operator.common.operator.resource.ConfigParameterParser.LONG;
import static io.strimzi.operator.common.operator.resource.ConfigParameterParser.INTEGER;
import static io.strimzi.operator.common.operator.resource.ConfigParameterParser.LOCAL_OBJECT_REFERENCE_LIST;
import static io.strimzi.operator.common.operator.resource.ConfigParameterParser.NAMESPACE_SET;
import static io.strimzi.operator.common.operator.resource.ConfigParameterParser.STRING;
import static io.strimzi.operator.common.operator.resource.ConfigParameterParser.LABEL_PREDICATE;
import static io.strimzi.operator.common.operator.resource.ConfigParameterParser.BOOLEAN;

/**
 * Cluster Operator configuration
 */
public class ClusterOperatorConfig {

    private static final Map<String, ConfigParameter<?>> CONFIG_VALUES = new HashMap<>();

    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorConfig.class.getName());

    // Env vars for configuring images
    /**
     * Configures the Kafka container images
     */
    public static final ConfigParameter<String> STRIMZI_KAFKA_IMAGES =  new ConfigParameter<>("STRIMZI_KAFKA_IMAGES", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Kafka Connect container images
     */
    public static final ConfigParameter<String> STRIMZI_KAFKA_CONNECT_IMAGES =  new ConfigParameter<>("STRIMZI_KAFKA_CONNECT_IMAGES", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Kafka Mirror Maker container images
     */
    public static final ConfigParameter<String> STRIMZI_KAFKA_MIRROR_MAKER_IMAGES =  new ConfigParameter<>("STRIMZI_KAFKA_MIRROR_MAKER_IMAGES", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Kafka Mirror Maker 2 container images
     */
    public static final ConfigParameter<String> STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES =  new ConfigParameter<>("STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Entity Operator TLS sidecar container images
     */
    public static final ConfigParameter<String> STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_TLS_SIDECAR_ENTITY_OPERATOR_IMAGE", STRING, "", CONFIG_VALUES);
    private static final ConfigParameter<String> STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE", STRING, "", CONFIG_VALUES); // Used only to produce warning if defined at startup
    private static final ConfigParameter<String> STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE", STRING, "", CONFIG_VALUES); // Used only to produce warning if defined at startup

    /**
     * Configures the Kafka Exporter container image
     */
    public static final ConfigParameter<String> KAFKA_EXPORTER_IMAGE = new ConfigParameter<>("STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Topic Operator container image
     */
    public static final ConfigParameter<String> STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE", STRING, "", CONFIG_VALUES);

    /**
     * Configures the User Operator container image
     */
    public static final ConfigParameter<String> STRIMZI_DEFAULT_USER_OPERATOR_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_USER_OPERATOR_IMAGE", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Kafka init container image
     */
    public static final ConfigParameter<String> STRIMZI_DEFAULT_KAFKA_INIT_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_KAFKA_INIT_IMAGE", STRING, "", CONFIG_VALUES);

    /**
     * Configures the HTTP Bridge container image
     */
    public static final ConfigParameter<String> STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Cruise Control container image
     */
    public static final ConfigParameter<String> STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Kaniko container image
     */
    public static final ConfigParameter<String> STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE =  new ConfigParameter<>("STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE", STRING, "", CONFIG_VALUES);

    /**
     * Configures the Maven container image
     */
    public static final ConfigParameter<String> STRIMZI_DEFAULT_MAVEN_BUILDER =  new ConfigParameter<>("STRIMZI_DEFAULT_MAVEN_BUILDER", STRING, "", CONFIG_VALUES);

    // Env vars configured in the Cluster operator deployment but passed to all operands
    /**
     * HTTP Proxy
     */
    public static final ConfigParameter<String> HTTP_PROXY =  new ConfigParameter<>("HTTP_PROXY", STRING, "", CONFIG_VALUES);

    /**
     * HTTPS Proxy
     */
    public static final ConfigParameter<String> HTTPS_PROXY =  new ConfigParameter<>("HTTPS_PROXY", STRING, "", CONFIG_VALUES);

    /**
     * Server which should not use proxy to connect to
     */
    public static final ConfigParameter<String> NO_PROXY =  new ConfigParameter<>("NO_PROXY", STRING, "", CONFIG_VALUES);

    /**
     * Enabled or disables the FIPS mode
     */
    public static final ConfigParameter<String> FIPS_MODE =  new ConfigParameter<>("FIPS_MODE", STRING, "", CONFIG_VALUES);

    // Default values

    /* test */ static final ConfigParameter<Set<String>> NAMESPACE = new ConfigParameter<>("STRIMZI_NAMESPACE", NAMESPACE_SET, "*",  CONFIG_VALUES);

    /* test */ public static final ConfigParameter<Long> FULL_RECONCILIATION_INTERVAL_MS = new ConfigParameter<>("STRIMZI_FULL_RECONCILIATION_INTERVAL_MS", LONG, "120000", CONFIG_VALUES);

    /**
     * Default work queue size for the Pod Set controller
     */
    public static final ConfigParameter<Integer> POD_SET_CONTROLLER_WORK_QUEUE_SIZE = new ConfigParameter<>("STRIMZI_POD_SET_CONTROLLER_WORK_QUEUE_SIZE", INTEGER, "1024", CONFIG_VALUES);
    /* test */ static final ConfigParameter<FeatureGates> STRIMZI_FEATURE_GATES = new ConfigParameter<>("STRIMZI_FEATURE_GATES", parseFeatureGates(), "", CONFIG_VALUES);


    /**
     * Default operations timeout
     */
    public static final ConfigParameter<Long> OPERATION_TIMEOUT_MS = new ConfigParameter<>("STRIMZI_OPERATION_TIMEOUT_MS", LONG, "300000", CONFIG_VALUES);
    public static final ConfigParameter<String> OPERATOR_NAME = new ConfigParameter<>("STRIMZI_OPERATOR_NAME", STRING, "cluster-operator-name-unset", CONFIG_VALUES);
    public static final ConfigParameter<Integer> ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS = new ConfigParameter<>("STRIMZI_ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS", INTEGER, "10000", CONFIG_VALUES);
    /* test */ static final ConfigParameter<Long> CONNECT_BUILD_TIMEOUT_MS = new ConfigParameter<>("STRIMZI_CONNECT_BUILD_TIMEOUT_MS", LONG, "300000", CONFIG_VALUES);
    public static final ConfigParameter<Integer> OPERATIONS_THREAD_POOL_SIZE = new ConfigParameter<>("STRIMZI_OPERATIONS_THREAD_POOL_SIZE", INTEGER, "10", CONFIG_VALUES);
    /* test */ static final ConfigParameter<Integer> DNS_CACHE_TTL = new ConfigParameter<>("STRIMZI_DNS_CACHE_TTL", INTEGER, "30", CONFIG_VALUES);
    public static final ConfigParameter<Boolean> NETWORK_POLICY_GENERATION = new ConfigParameter<>("STRIMZI_NETWORK_POLICY_GENERATION", BOOLEAN, "true", CONFIG_VALUES);
    public static final ConfigParameter<Boolean> CREATE_CLUSTER_ROLES = new ConfigParameter<>("STRIMZI_CREATE_CLUSTER_ROLES", BOOLEAN, "false", CONFIG_VALUES);
    public static final ConfigParameter<Boolean> POD_SET_RECONCILIATION_ONLY = new ConfigParameter<>("STRIMZI_POD_SET_RECONCILIATION_ONLY", BOOLEAN, "false", CONFIG_VALUES);

    /* test */ static final ConfigParameter<List<LocalObjectReference>> IMAGE_PULL_SECRETS = new ConfigParameter<>("STRIMZI_IMAGE_PULL_SECRETS", LOCAL_OBJECT_REFERENCE_LIST, null, CONFIG_VALUES);
    /* test */ static final ConfigParameter<String> OPERATOR_NAMESPACE = new ConfigParameter<>("STRIMZI_OPERATOR_NAMESPACE", STRING, null, CONFIG_VALUES);
    /* test */ public static final ConfigParameter<Labels> OPERATOR_NAMESPACE_LABELS = new ConfigParameter<>("STRIMZI_OPERATOR_NAMESPACE_LABELS", LABEL_PREDICATE, null, CONFIG_VALUES);
    /* test */ static final ConfigParameter<ImagePullPolicy> IMAGE_PULL_POLICY = new ConfigParameter<>("STRIMZI_IMAGE_PULL_POLICY", parseImagePullPolicy(), null, CONFIG_VALUES);

    /* test */ public static final ConfigParameter<Labels> CUSTOM_RESOURCE_SELECTOR = new ConfigParameter<>("STRIMZI_CUSTOM_RESOURCE_SELECTOR", LABEL_PREDICATE, null, CONFIG_VALUES);

    /**
     * Default Pod Security Provider class
     */
    public static final ConfigParameter<String> POD_SECURITY_PROVIDER_CLASS = new ConfigParameter<>("STRIMZI_POD_SECURITY_PROVIDER_CLASS", STRING, "io.strimzi.plugin.security.profiles.impl.BaselinePodSecurityProvider", CONFIG_VALUES);

    public static final ConfigParameter<Boolean> LEADER_ELECTION_ENABLED = new ConfigParameter<>("STRIMZI_LEADER_ELECTION_ENABLED", BOOLEAN, "false", CONFIG_VALUES);

    // PodSecurityPolicy shortcut keywords and the corresponding class names
    public static final ConfigParameter<String> POD_SECURITY_PROVIDER_BASELINE_SHORTCUT = new ConfigParameter<>("POD_SECURITY_PROVIDER_BASELINE_SHORTCUT", STRING, "baseline", CONFIG_VALUES);
    /* test */ static final ConfigParameter<String> POD_SECURITY_PROVIDER_BASELINE_CLASS = new ConfigParameter<>("POD_SECURITY_PROVIDER_BASELINE_CLASS", STRING, "io.strimzi.plugin.security.profiles.impl.BaselinePodSecurityProvider", CONFIG_VALUES);
    public static final ConfigParameter<String> POD_SECURITY_PROVIDER_RESTRICTED_SHORTCUT = new ConfigParameter<>("POD_SECURITY_PROVIDER_RESTRICTED_SHORTCUT", STRING, "restricted", CONFIG_VALUES);
    /* test */ static final ConfigParameter<String> POD_SECURITY_PROVIDER_RESTRICTED_CLASS = new ConfigParameter<>("POD_SECURITY_PROVIDER_RESTRICTED_CLASS", STRING, "io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider", CONFIG_VALUES);
    private final KafkaVersion.Lookup versions;

    /**
     * Constructor
     *
     * @param namespaces                    namespace in which the operator will run and create resources
     * @param reconciliationIntervalMs      specify every how many milliseconds the reconciliation runs
     * @param operationTimeoutMs            timeout for internal operations specified in milliseconds
     * @param connectBuildTimeoutMs         timeout used to wait for a Kafka Connect builds to finish
     * @param createClusterRoles            true to create the ClusterRoles
     * @param networkPolicyGeneration       true to generate Network Policies
     * @param versions                      The configured Kafka versions
     * @param imagePullPolicy               Image pull policy configured by the user
     * @param imagePullSecrets              Set of secrets for pulling container images from secured repositories
     * @param operatorNamespace             Name of the namespace in which the operator is running
     * @param operatorNamespaceLabels       Labels of the namespace in which the operator is running (used for network policies)
     * @param customResourceSelector        Labels used to filter the custom resources seen by the cluster operator
     * @param featureGates                  Configuration string with feature gates settings
     * @param operationsThreadPoolSize      The size of the thread pool used for various operations
     * @param zkAdminSessionTimeoutMs       Session timeout for the Zookeeper Admin client used in ZK scaling operations
     * @param dnsCacheTtlSec                Number of seconds to cache a successful DNS name lookup
     * @param podSetReconciliationOnly      Indicates whether this Cluster Operator instance should reconcile only the
     *                                      StrimziPodSet resources or not
     * @param podSetControllerWorkQueueSize Indicates the size of the StrimziPodSetController work queue
     * @param operatorName                  The Pod name of the cluster operator, used to identify source of K8s events the operator creates
     * @param podSecurityProviderClass      The PodSecurityProvider class which the operator should use
     * @param leaderElectionConfig          Configuration of the Cluster Operator leader election
     */


    /**
     * Logs warnings for removed / deprecated environment variables
     *
     * @param map   map from which loading configuration parameters
     */
    private static void warningsForRemovedEndVars(Map<String, String> map) {
        if (map.containsKey(STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE.key()))    {
            LOGGER.warn("Kafka TLS sidecar container has been removed and the environment variable {} is not used anymore. " +
                    "You can remove it from the Strimzi Cluster Operator deployment.", STRIMZI_DEFAULT_TLS_SIDECAR_KAFKA_IMAGE);
        }
        if (map.containsKey(STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE.key()))    {
            LOGGER.warn("Cruise Control TLS sidecar container has been removed and the environment variable {} is not used anymore. " +
                    "You can remove it from the Strimzi Cluster Operator deployment.", STRIMZI_DEFAULT_TLS_SIDECAR_CRUISE_CONTROL_IMAGE);
        }
    }

    /**
     * Loads configuration parameters from a related map
     * This is used for testing.
     *
     * @param map   map from which loading configuration parameters
     * @return  Cluster Operator configuration instance
     */

    public static ClusterOperatorConfig buildFromMap(Map<String, String> map) {
        warningsForRemovedEndVars(map);
        KafkaVersion.Lookup lookup = parseKafkaVersions(map.get(STRIMZI_KAFKA_IMAGES.key()), map.get(STRIMZI_KAFKA_CONNECT_IMAGES.key()), map.get(STRIMZI_KAFKA_MIRROR_MAKER_IMAGES.key()), map.get(STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES.key()));
        return buildFromMap(map, lookup);

    }

    public static ClusterOperatorConfig buildFromMap(Map<String, String> map, KafkaVersion.Lookup lookup) {

        Map<String, String> envMap = new HashMap<>(map);
        CONFIG_VALUES.putAll(LeaderElectionManagerConfig.CONFIG_VALUES);
        envMap.keySet().retainAll(ClusterOperatorConfig.keyNames());

        Map<String, Object> generatedMap = ConfigParameter.define(envMap, CONFIG_VALUES);
        return new ClusterOperatorConfig(generatedMap, lookup);
    }

    private final Map<String, Object> map;

    /**
     * Constructor
     *
     * @param map Map containing configurations and their respective values
     */

    private ClusterOperatorConfig(Map<String, Object> map, KafkaVersion.Lookup lookup) {
        this.versions = lookup;
        this.map = map;
    }

    /**
     * @return Set of configuration key/names
     */
    public static Set<String> keyNames() {
        return Collections.unmodifiableSet(CONFIG_VALUES.keySet());
    }

    /**
     * Gets the configuration value corresponding to the key
     * @param <T>      Type of value
     * @param value    Instance of Config Parameter class
     * @return         Configuration value w.r.t to the key
     */
    @SuppressWarnings("unchecked")
    public <T> T get(ConfigParameter<T> value) {
        return (T) this.map.get(value.key());
    }


    /* test */ static boolean parseBoolean(String envVar, boolean defaultValue) {
        boolean value = defaultValue;

        if (envVar != null) {
            value = Boolean.parseBoolean(envVar);
        }

        return value;
    }

    private static KafkaVersion.Lookup parseKafkaVersions(String kafkaImages, String connectImages, String mirrorMakerImages, String mirrorMaker2Images) {
        KafkaVersion.Lookup lookup = new KafkaVersion.Lookup(
                Util.parseMap(kafkaImages),
                Util.parseMap(connectImages),
                Util.parseMap(mirrorMakerImages),
                Util.parseMap(mirrorMaker2Images));

        String image = "";
        String envVar = "";

        try {
            image = "Kafka";
            envVar = STRIMZI_KAFKA_IMAGES.key();
            lookup.validateKafkaImages(lookup.supportedVersions());

            image = "Kafka Connect";
            envVar = STRIMZI_KAFKA_CONNECT_IMAGES.key();
            lookup.validateKafkaConnectImages(lookup.supportedVersions());

            image = "Kafka Mirror Maker";
            envVar = STRIMZI_KAFKA_MIRROR_MAKER_IMAGES.key();
            lookup.validateKafkaMirrorMakerImages(lookup.supportedVersions());

            image = "Kafka Mirror Maker 2";
            envVar = STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES.key();
            lookup.validateKafkaMirrorMaker2Images(lookup.supportedVersionsForFeature("kafkaMirrorMaker2"));
        } catch (NoImageException | UnsupportedVersionException e) {
            throw new InvalidConfigurationException("Failed to parse default container image configuration for " + image + " from environment variable " + envVar, e);
        }
        return lookup;
    }

    /**
     * Parse the configuration of the Pod Security Provider class which should be used to configure the Pod and
     * Container Security Contexts
     *
     * @param envVar The value of the environment variable configuring the Pod Security Provider
     * @return The full name of the class which should be used as the Pod security Provider
     */
    /* test */ static String parsePodSecurityProviderClass(String envVar) {
        String value = envVar != null ? envVar : POD_SECURITY_PROVIDER_CLASS.key();

        if (POD_SECURITY_PROVIDER_BASELINE_SHORTCUT.equals(value.toLowerCase(Locale.ENGLISH)))  {
            return POD_SECURITY_PROVIDER_BASELINE_CLASS.defaultValue();
        } else if (POD_SECURITY_PROVIDER_RESTRICTED_SHORTCUT.equals(value.toLowerCase(Locale.ENGLISH)))  {
            return POD_SECURITY_PROVIDER_RESTRICTED_CLASS.defaultValue();
        } else {
            return value;
        }
    }

    static ConfigParameterParser<FeatureGates> parseFeatureGates() {
        return FeatureGates::new;
    };


    static ConfigParameterParser<ImagePullPolicy> parseImagePullPolicy() {
        return imagePullPolicyEnvVar -> {
            ImagePullPolicy imagePullPolicy = null;

            if (imagePullPolicyEnvVar != null) {
                switch (imagePullPolicyEnvVar.trim().toLowerCase(Locale.ENGLISH)) {
                    case "always":
                        imagePullPolicy = ImagePullPolicy.ALWAYS;
                        break;
                    case "ifnotpresent":
                        imagePullPolicy = ImagePullPolicy.IFNOTPRESENT;
                        break;
                    case "never":
                        imagePullPolicy = ImagePullPolicy.NEVER;
                        break;
                    default:
                        throw new InvalidConfigurationException(imagePullPolicyEnvVar
                                + " is not a valid. It can only have one of the following values: Always, IfNotPresent, Never.");
                }
            }
            return imagePullPolicy;
        };
    }

    public static class ClusterOperatorConfigBuilder {

        private final Map<String, Object> map;
        KafkaVersion.Lookup versions;

        public ClusterOperatorConfigBuilder(ClusterOperatorConfig config, KafkaVersion.Lookup lookup) {
            this.map = config.map;
            this.versions = lookup;
        }

        public ClusterOperatorConfigBuilder with(String key, String value) {
            this.map.put(key, CONFIG_VALUES.get(key).type().parse(value));
            return this;
        }

        public ClusterOperatorConfig build() {
            return new ClusterOperatorConfig(this.map, versions);
        }
    }


    /**
     * @return  namespaces in which the operator runs and creates resources
     */
    public Set<String> getNamespaces() {
        return get(NAMESPACE);
    }

    /**
     * @return  how many milliseconds the reconciliation runs
     */
    public long getReconciliationIntervalMs() {
        return get(FULL_RECONCILIATION_INTERVAL_MS);
    }

    /**
     * @return  how many milliseconds should we wait for Kubernetes operations
     */
    public long getOperationTimeoutMs() {
        return get(OPERATION_TIMEOUT_MS);
    }

    /**
     * @return  how many milliseconds should we wait for Zookeeper Admin Sessions to timeout
     */
    public int getZkAdminSessionTimeoutMs() {
        return get(ZOOKEEPER_ADMIN_SESSION_TIMEOUT_MS);
    }

    /**
     * @return  How many milliseconds should we wait for Kafka Connect build to complete
     */
    public long getConnectBuildTimeoutMs() {
        return get(CONNECT_BUILD_TIMEOUT_MS);
    }

    /**
     * @return  Indicates whether Cluster Roles should be created
     */
    public boolean isCreateClusterRoles() {
        return get(CREATE_CLUSTER_ROLES);
    }

    /**
     * @return  Indicates whether Network policies should be generated
     */
    public boolean isNetworkPolicyGeneration() {
        return get(NETWORK_POLICY_GENERATION);
    }

    /**
     * @return  Supported Kafka versions and informations about them
     */
    public KafkaVersion.Lookup versions() {
        return versions;
    }

    /**
     * @return  The user-configure image pull policy. Null if it was not configured.
     */
    public ImagePullPolicy getImagePullPolicy() {
        return get(IMAGE_PULL_POLICY);
    }

    /**
     * @return The list of configured ImagePullSecrets. Null if no secrets were configured.
     */
    public List<LocalObjectReference> getImagePullSecrets() {
        return get(IMAGE_PULL_SECRETS);
    }

    /**
     * @return Returns the name of the namespace where the operator runs or null if not configured
     */
    public String getOperatorNamespace() {
        return get(OPERATOR_NAMESPACE);
    }

    /**
     * @return Returns the labels of the namespace where the operator runs or null if not configured
     */
    public Labels getOperatorNamespaceLabels() {
        return get(OPERATOR_NAMESPACE_LABELS);
    }

    /**
     * @return Labels used for filtering custom resources
     */
    public Labels getCustomResourceSelector() {
        return get(CUSTOM_RESOURCE_SELECTOR);
    }

    /**
     * @return  Feature gates configuration
     */
    public FeatureGates featureGates()  {
        return get(STRIMZI_FEATURE_GATES);
    }

    /**
     * @return Thread Pool size to be used by the operator to do operations like reconciliation
     */
    public int getOperationsThreadPoolSize() {
        return get(OPERATIONS_THREAD_POOL_SIZE);
    }

    /**
     * @return Number of seconds to cache a successful DNS name lookup
     */
    public int getDnsCacheTtlSec() {
        return get(DNS_CACHE_TTL);
    }

    /**
     * @return Indicates whether this Cluster Operator instance should reconcile only the StrimziPodSet resources or not
     */
    public boolean isPodSetReconciliationOnly() {
        return get(POD_SET_RECONCILIATION_ONLY);
    }

    /**
     * @return Returns the size of the StrimziPodSetController work queue
     */
    public int getPodSetControllerWorkQueueSize() {
        return get(POD_SET_CONTROLLER_WORK_QUEUE_SIZE);
    }

    /**
     * @return  The name of this operator
     */
    public String getOperatorName() {
        return get(OPERATOR_NAME);
    }

    /**
     * @return Returns the Pod Security Provider class
     */
    public String getPodSecurityProviderClass() {
        return parsePodSecurityProviderClass(get(POD_SECURITY_PROVIDER_CLASS));
    }

    /**
     * @return Returns the Leader Election Manager configuration
     */
    public LeaderElectionManagerConfig getLeaderElectionConfig() {
        if (get(LEADER_ELECTION_ENABLED)) {
            return LeaderElectionManagerConfig.buildFromExistingMap(this.map);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "ClusterOperatorConfig(" +
                "namespaces=" + getNamespaces() +
                ",reconciliationIntervalMs=" + getReconciliationIntervalMs() +
                ",operationTimeoutMs=" + getOperationTimeoutMs() +
                ",connectBuildTimeoutMs=" + getConnectBuildTimeoutMs() +
                ",createClusterRoles=" + isCreateClusterRoles() +
                ",networkPolicyGeneration=" + isNetworkPolicyGeneration() +
                ",versions=" + versions() +
                ",imagePullPolicy=" + getImagePullPolicy() +
                ",imagePullSecrets=" + getImagePullSecrets() +
                ",operatorNamespace=" + getOperatorNamespace() +
                ",operatorNamespaceLabels=" + getOperatorNamespaceLabels() +
                ",customResourceSelector=" + getCustomResourceSelector() +
                ",featureGates=" + featureGates() +
                ",zkAdminSessionTimeoutMs=" + getZkAdminSessionTimeoutMs() +
                ",dnsCacheTtlSec=" + getDnsCacheTtlSec() +
                ",podSetReconciliationOnly=" + isPodSetReconciliationOnly() +
                ",podSetControllerWorkQueueSize=" + getPodSetControllerWorkQueueSize() +
                ",operatorName=" + getOperatorName() +
                ",podSecurityProviderClass=" + getPodSecurityProviderClass() +
                ",leaderElectionConfig=" + getLeaderElectionConfig() +
                ")";
    }
}
