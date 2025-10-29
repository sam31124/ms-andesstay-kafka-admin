package cl.duoc.andesstay.kafka.dto;

import java.util.Map;

public class TopicDetailResponse {

    private String name;
    private int partitions;
    private int replicationFactor;
    private boolean internal;
    private String cleanupPolicy;
    private String retentionMs;
    private Map<String, String> configs;

    public TopicDetailResponse() {
    }

    public TopicDetailResponse(String name, int partitions, int replicationFactor, boolean internal,
                               String cleanupPolicy, String retentionMs, Map<String, String> configs) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.internal = internal;
        this.cleanupPolicy = cleanupPolicy;
        this.retentionMs = retentionMs;
        this.configs = configs;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }

    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

    public boolean isInternal() { return internal; }
    public void setInternal(boolean internal) { this.internal = internal; }

    public String getCleanupPolicy() { return cleanupPolicy; }
    public void setCleanupPolicy(String cleanupPolicy) { this.cleanupPolicy = cleanupPolicy; }

    public String getRetentionMs() { return retentionMs; }
    public void setRetentionMs(String retentionMs) { this.retentionMs = retentionMs; }

    public Map<String, String> getConfigs() { return configs; }
    public void setConfigs(Map<String, String> configs) { this.configs = configs; }
}
