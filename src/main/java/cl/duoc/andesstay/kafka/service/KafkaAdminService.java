package cl.duoc.andesstay.kafka.service;

import cl.duoc.andesstay.kafka.config.KafkaAdminConfig;
import cl.duoc.andesstay.kafka.dto.ConsumerGroupInfo;
import cl.duoc.andesstay.kafka.dto.CreateTopicRequest;
import cl.duoc.andesstay.kafka.dto.TopicDetailResponse;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public List<TopicDetailResponse> listTopics() {
        try {
            ListTopicsResult topicsResult = adminClient.listTopics(new ListTopicsOptions().listInternal(false));
            Set<String> topicNames = topicsResult.names().get(5, TimeUnit.SECONDS);

            if (topicNames.isEmpty()) {
                return Collections.emptyList();
            }

            DescribeTopicsResult describeResult = adminClient.describeTopics(topicNames);
            Map<String, TopicDescription> descriptions = describeResult.allTopicNames().get(5, TimeUnit.SECONDS);

            List<ConfigResource> resources = topicNames.stream()
                    .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                    .toList();

            DescribeConfigsResult configsResult = adminClient.describeConfigs(resources);
            Map<ConfigResource, Config> configsMap = configsResult.all().get(5, TimeUnit.SECONDS);

            List<TopicDetailResponse> result = new ArrayList<>();
            for (String name : topicNames) {
                TopicDescription desc = descriptions.get(name);
                ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, name);
                Config cfg = configsMap.get(res);

                int partitions = desc != null ? desc.partitions().size() : 0;
                int replication = (desc != null && !desc.partitions().isEmpty())
                        ? desc.partitions().get(0).replicas().size() : 1;

                String retention = (cfg != null && cfg.get(TopicConfig.RETENTION_MS_CONFIG) != null)
                        ? cfg.get(TopicConfig.RETENTION_MS_CONFIG).value() : "default";

                String cleanup = (cfg != null && cfg.get(TopicConfig.CLEANUP_POLICY_CONFIG) != null)
                        ? cfg.get(TopicConfig.CLEANUP_POLICY_CONFIG).value() : "delete";

                Map<String, String> customConfigs = new HashMap<>();
                if (cfg != null) {
                    cfg.entries().forEach(entry -> {
                        if (!entry.isDefault()) {
                            customConfigs.put(entry.name(), entry.value());
                        }
                    });
                }

                result.add(new TopicDetailResponse(
                        name,
                        partitions,
                        replication,
                        desc != null && desc.isInternal(),
                        cleanup,
                        retention,
                        customConfigs
                ));
            }

            result.sort(Comparator.comparing(TopicDetailResponse::getName));
            return result;
        } catch (Exception e) {
            log.error("Error al listar tópicos de Kafka: {}", e.getMessage());
            throw new RuntimeException("No se pudieron listar los tópicos de Kafka: " + e.getMessage(), e);
        }
    }

    public TopicDetailResponse getTopicDetail(String topicName) {
        try {
            DescribeTopicsResult descResult = adminClient.describeTopics(Collections.singletonList(topicName));
            TopicDescription desc = descResult.allTopicNames().get(5, TimeUnit.SECONDS).get(topicName);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            DescribeConfigsResult cfgResult = adminClient.describeConfigs(Collections.singletonList(resource));
            Config cfg = cfgResult.all().get(5, TimeUnit.SECONDS).get(resource);

            int partitions = desc.partitions().size();
            int replication = desc.partitions().isEmpty() ? 1 : desc.partitions().get(0).replicas().size();

            String retention = cfg.get(TopicConfig.RETENTION_MS_CONFIG) != null
                    ? cfg.get(TopicConfig.RETENTION_MS_CONFIG).value() : "default";
            String cleanup = cfg.get(TopicConfig.CLEANUP_POLICY_CONFIG) != null
                    ? cfg.get(TopicConfig.CLEANUP_POLICY_CONFIG).value() : "delete";

            Map<String, String> customConfigs = new HashMap<>();
            cfg.entries().forEach(entry -> customConfigs.put(entry.name(), entry.value()));

            return new TopicDetailResponse(
                    topicName,
                    partitions,
                    replication,
                    desc.isInternal(),
                    cleanup,
                    retention,
                    customConfigs
            );
        } catch (Exception e) {
            log.error("Error al obtener detalle del tópico '{}': {}", topicName, e.getMessage());
            throw new RuntimeException("Error al consultar tópico '" + topicName + "': " + e.getMessage(), e);
        }
    }

    public TopicDetailResponse createTopic(CreateTopicRequest request) {
        try {
            Map<String, String> configs = new HashMap<>();
            if (request.getRetentionMs() != null) {
                configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(request.getRetentionMs()));
            }
            if (request.getCleanupPolicy() != null && !request.getCleanupPolicy().isBlank()) {
                configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, request.getCleanupPolicy().trim().toLowerCase());
            }

            NewTopic newTopic = new NewTopic(request.getName(), request.getPartitions(), request.getReplicationFactor());
            newTopic.configs(configs);

            CreateTopicsResult createResult = adminClient.createTopics(Collections.singletonList(newTopic));
            createResult.all().get(10, TimeUnit.SECONDS);

            log.info("Tópico '{}' creado exitosamente con {} particiones y retención {} ms",
                    request.getName(), request.getPartitions(), request.getRetentionMs());

            return getTopicDetail(request.getName());
        } catch (Exception e) {
            log.error("Error al crear el tópico '{}': {}", request.getName(), e.getMessage());
            throw new RuntimeException("No se pudo crear el tópico '" + request.getName() + "': " + e.getMessage(), e);
        }
    }

    public void deleteTopic(String topicName) {
        try {
            DeleteTopicsResult deleteResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            deleteResult.all().get(10, TimeUnit.SECONDS);
            log.info("Tópico '{}' eliminado exitosamente", topicName);
        } catch (Exception e) {
            log.error("Error al eliminar el tópico '{}': {}", topicName, e.getMessage());
            throw new RuntimeException("No se pudo eliminar el tópico '" + topicName + "': " + e.getMessage(), e);
        }
    }

    public void updateTopicRetention(String topicName, Long retentionMs, String cleanupPolicy) {
        try {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Collection<AlterConfigOp> alterOps = new ArrayList<>();

            if (retentionMs != null) {
                alterOps.add(new AlterConfigOp(
                        new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs)),
                        AlterConfigOp.OpType.SET
                ));
            }
            if (cleanupPolicy != null && !cleanupPolicy.isBlank()) {
                alterOps.add(new AlterConfigOp(
                        new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy.trim().toLowerCase()),
                        AlterConfigOp.OpType.SET
                ));
            }

            Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(resource, alterOps);
            AlterConfigsResult alterResult = adminClient.incrementalAlterConfigs(configs);
            alterResult.all().get(10, TimeUnit.SECONDS);

            log.info("Parámetros de retención para tópico '{}' actualizados a retentionMs={}, cleanup={}",
                    topicName, retentionMs, cleanupPolicy);
        } catch (Exception e) {
            log.error("Error al actualizar configuración del tópico '{}': {}", topicName, e.getMessage());
            throw new RuntimeException("No se pudo actualizar configuración del tópico: " + e.getMessage(), e);
        }
    }

    public List<ConsumerGroupInfo> listConsumerGroups() {
        try {
            ListConsumerGroupsResult groupsResult = adminClient.listConsumerGroups();
            Collection<ConsumerGroupListing> listings = groupsResult.all().get(5, TimeUnit.SECONDS);

            List<ConsumerGroupInfo> result = new ArrayList<>();
            for (ConsumerGroupListing group : listings) {
                result.add(new ConsumerGroupInfo(
                        group.groupId(),
                        group.state().map(Enum::name).orElse("UNKNOWN"),
                        0,
                        group.isSimpleConsumerGroup()
                ));
            }
            return result;
        } catch (Exception e) {
            log.warn("Advertencia al consultar Consumer Groups: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public Map<String, Object> initOfficialTopics() {
        Map<String, Object> response = new HashMap<>();
        List<String> created = new ArrayList<>();

        // Tópico 1: andesstay.reservations.stream
        try {
            createTopic(new CreateTopicRequest(
                    KafkaAdminConfig.TOPIC_RESERVATIONS_STREAM,
                    3,
                    (short) 1,
                    604800000L,
                    "delete"
            ));
            created.add(KafkaAdminConfig.TOPIC_RESERVATIONS_STREAM + " (3 particiones, retención 7 días)");
        } catch (Exception ex) {
            log.info("Tópico '{}' ya existe o se validó.", KafkaAdminConfig.TOPIC_RESERVATIONS_STREAM);
        }

        // Tópico 2: andesstay.analytics.telemetry
        try {
            createTopic(new CreateTopicRequest(
                    KafkaAdminConfig.TOPIC_ANALYTICS_TELEMETRY,
                    2,
                    (short) 1,
                    2592000000L,
                    "compact"
            ));
            created.add(KafkaAdminConfig.TOPIC_ANALYTICS_TELEMETRY + " (2 particiones, retención 30 días)");
        } catch (Exception ex) {
            log.info("Tópico '{}' ya existe o se validó.", KafkaAdminConfig.TOPIC_ANALYTICS_TELEMETRY);
        }

        response.put("status", "SUCCESS");
        response.put("message", "Topología oficial de Kafka AndesStay inicializada.");
        response.put("topicsCreatedOrVerified", created);
        return response;
    }

    public Map<String, Object> getClusterStatus() {
        try {
            DescribeClusterResult cluster = adminClient.describeCluster();
            String clusterId = cluster.clusterId().get(5, TimeUnit.SECONDS);
            int nodes = cluster.nodes().get(5, TimeUnit.SECONDS).size();
            String controller = String.valueOf(cluster.controller().get(5, TimeUnit.SECONDS));

            return Map.of(
                    "status", "CONNECTED",
                    "clusterId", clusterId,
                    "nodesCount", nodes,
                    "controller", controller
            );
        } catch (Exception e) {
            return Map.of(
                    "status", "DISCONNECTED",
                    "error", e.getMessage()
            );
        }
    }
}
