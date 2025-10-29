package cl.duoc.andesstay.kafka;

import cl.duoc.andesstay.kafka.controller.KafkaAdminController;
import cl.duoc.andesstay.kafka.dto.CreateTopicRequest;
import cl.duoc.andesstay.kafka.dto.TopicDetailResponse;
import cl.duoc.andesstay.kafka.service.KafkaAdminService;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaAdminController.class)
class MsAndesstayKafkaAdminApplicationTests {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private KafkaAdminService kafkaAdminService;

    @MockBean
    private AdminClient adminClient;

    @Test
    @DisplayName("GET /api/kafka/status - Debería retornar estado del clúster")
    void testGetStatus() throws Exception {
        when(kafkaAdminService.getClusterStatus()).thenReturn(Map.of("status", "CONNECTED", "nodesCount", 1));

        mockMvc.perform(get("/api/kafka/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CONNECTED"))
                .andExpect(jsonPath("$.nodesCount").value(1));
    }

    @Test
    @DisplayName("GET /api/kafka/topics - Debería listar tópicos")
    void testListTopics() throws Exception {
        TopicDetailResponse topic = new TopicDetailResponse(
                "andesstay.reservations.stream",
                3,
                1,
                false,
                "delete",
                "604800000",
                Collections.emptyMap()
        );
        when(kafkaAdminService.listTopics()).thenReturn(List.of(topic));

        mockMvc.perform(get("/api/kafka/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("andesstay.reservations.stream"))
                .andExpect(jsonPath("$[0].partitions").value(3))
                .andExpect(jsonPath("$[0].retentionMs").value("604800000"));
    }

    @Test
    @DisplayName("POST /api/kafka/topics - Debería crear un nuevo tópico")
    void testCreateTopic() throws Exception {
        TopicDetailResponse created = new TopicDetailResponse(
                "andesstay.test.topic",
                2,
                1,
                false,
                "delete",
                "3600000",
                Collections.emptyMap()
        );
        when(kafkaAdminService.createTopic(any(CreateTopicRequest.class))).thenReturn(created);

        String json = """
                {
                    "name": "andesstay.test.topic",
                    "partitions": 2,
                    "replicationFactor": 1,
                    "retentionMs": 3600000,
                    "cleanupPolicy": "delete"
                }
                """;

        mockMvc.perform(post("/api/kafka/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("andesstay.test.topic"))
                .andExpect(jsonPath("$.partitions").value(2));
    }

    @Test
    @DisplayName("DELETE /api/kafka/topics/{name} - Debería eliminar tópico")
    void testDeleteTopic() throws Exception {
        doNothing().when(kafkaAdminService).deleteTopic("andesstay.test.topic");

        mockMvc.perform(delete("/api/kafka/topics/andesstay.test.topic"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));

        verify(kafkaAdminService, times(1)).deleteTopic("andesstay.test.topic");
    }

    @Test
    @DisplayName("POST /api/kafka/topics/init - Debería inicializar tópicos oficiales")
    void testInitOfficialTopics() throws Exception {
        when(kafkaAdminService.initOfficialTopics()).thenReturn(Map.of(
                "status", "SUCCESS",
                "message", "Topología oficial de Kafka AndesStay inicializada."
        ));

        mockMvc.perform(post("/api/kafka/topics/init"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"));
    }
}
