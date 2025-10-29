package cl.duoc.andesstay.kafka.controller;

import cl.duoc.andesstay.kafka.dto.ConsumerGroupInfo;
import cl.duoc.andesstay.kafka.dto.CreateTopicRequest;
import cl.duoc.andesstay.kafka.dto.TopicDetailResponse;
import cl.duoc.andesstay.kafka.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
@CrossOrigin(origins = "*")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        return ResponseEntity.ok(kafkaAdminService.getClusterStatus());
    }

    @GetMapping("/topics")
    public ResponseEntity<List<TopicDetailResponse>> listTopics() {
        return ResponseEntity.ok(kafkaAdminService.listTopics());
    }

    @GetMapping("/topics/{name}")
    public ResponseEntity<TopicDetailResponse> getTopicDetail(@PathVariable String name) {
        return ResponseEntity.ok(kafkaAdminService.getTopicDetail(name));
    }

    @PostMapping("/topics")
    public ResponseEntity<TopicDetailResponse> createTopic(@Valid @RequestBody CreateTopicRequest request) {
        TopicDetailResponse response = kafkaAdminService.createTopic(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @DeleteMapping("/topics/{name}")
    public ResponseEntity<Map<String, String>> deleteTopic(@PathVariable String name) {
        kafkaAdminService.deleteTopic(name);
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "message", "Tópico '" + name + "' eliminado correctamente de Kafka."
        ));
    }

    @PutMapping("/topics/{name}/retention")
    public ResponseEntity<Map<String, Object>> updateRetention(
            @PathVariable String name,
            @RequestParam(required = false) Long retentionMs,
            @RequestParam(required = false) String cleanupPolicy) {
        kafkaAdminService.updateTopicRetention(name, retentionMs, cleanupPolicy);
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "message", "Configuración de retención actualizada para tópico '" + name + "'.",
                "retentionMs", retentionMs != null ? retentionMs : "sin cambios",
                "cleanupPolicy", cleanupPolicy != null ? cleanupPolicy : "sin cambios"
        ));
    }

    @GetMapping("/consumer-groups")
    public ResponseEntity<List<ConsumerGroupInfo>> listConsumerGroups() {
        return ResponseEntity.ok(kafkaAdminService.listConsumerGroups());
    }

    @PostMapping("/topics/init")
    public ResponseEntity<Map<String, Object>> initOfficialTopics() {
        return ResponseEntity.ok(kafkaAdminService.initOfficialTopics());
    }
}
