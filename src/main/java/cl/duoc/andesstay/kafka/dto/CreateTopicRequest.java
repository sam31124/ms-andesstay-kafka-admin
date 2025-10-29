package cl.duoc.andesstay.kafka.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public class CreateTopicRequest {

    @NotBlank(message = "El nombre del tópico no puede estar vacío")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre del tópico solo puede contener letras, números, puntos, guiones y guiones bajos")
    private String name;

    @Min(value = 1, message = "Debe tener al menos 1 partición")
    private int partitions = 3;

    @Min(value = 1, message = "El factor de replicación debe ser al menos 1")
    private short replicationFactor = 1;

    private Long retentionMs = 604800000L; // 7 días por defecto

    private String cleanupPolicy = "delete"; // delete o compact

    public CreateTopicRequest() {
    }

    public CreateTopicRequest(String name, int partitions, short replicationFactor, Long retentionMs, String cleanupPolicy) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.retentionMs = retentionMs;
        this.cleanupPolicy = cleanupPolicy;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }

    public short getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(short replicationFactor) { this.replicationFactor = replicationFactor; }

    public Long getRetentionMs() { return retentionMs; }
    public void setRetentionMs(Long retentionMs) { this.retentionMs = retentionMs; }

    public String getCleanupPolicy() { return cleanupPolicy; }
    public void setCleanupPolicy(String cleanupPolicy) { this.cleanupPolicy = cleanupPolicy; }
}
