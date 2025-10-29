package cl.duoc.andesstay.kafka.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    public static final String TOPIC_RESERVATIONS_STREAM = "andesstay.reservations.stream";
    public static final String TOPIC_ANALYTICS_TELEMETRY = "andesstay.analytics.telemetry";

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        configs.put(AdminClientConfig.RETRIES_CONFIG, 3);
        return new KafkaAdmin(configs);
    }

    @Bean
    public AdminClient adminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        configs.put(AdminClientConfig.RETRIES_CONFIG, 3);
        return AdminClient.create(configs);
    }

    /**
     * Tópico Oficial 1: Flujo de eventos de reservas en tiempo real.
     * Particiones: 3 (Balanceo de carga entre microservicios)
     * Réplicas: 1
     * Retención: 7 días (604800000 ms)
     * Cleanup Policy: delete
     */
    @Bean
    public NewTopic reservationsStreamTopic() {
        return TopicBuilder.name(TOPIC_RESERVATIONS_STREAM)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    /**
     * Tópico Oficial 2: Telemetría y analítica de ocupación.
     * Particiones: 2
     * Réplicas: 1
     * Retención: 30 días (2592000000 ms)
     * Cleanup Policy: compact
     */
    @Bean
    public NewTopic analyticsTelemetryTopic() {
        return TopicBuilder.name(TOPIC_ANALYTICS_TELEMETRY)
                .partitions(2)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .build();
    }
}
