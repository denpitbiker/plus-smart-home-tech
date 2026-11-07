package ru.yandex.practicum.inventory;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class InventoryServiceEdgeCaseTest {
    @Autowired private MockMvc mvc;

    @Test
    void shouldPreserveStockAfterRejectedUpdatesAndReservations() throws Exception {
        String record = "{\"productId\":200001,\"quantity\":5}";
        mvc.perform(post("/api/inventory").contentType(MediaType.APPLICATION_JSON).content(record))
                .andExpect(status().isCreated());
        mvc.perform(post("/api/inventory").contentType(MediaType.APPLICATION_JSON).content(record))
                .andExpect(status().isConflict());
        mvc.perform(post("/api/inventory/reserve").contentType(MediaType.APPLICATION_JSON).content(record))
                .andExpect(status().isOk()).andExpect(jsonPath("$.availableQuantity").value(0));
        mvc.perform(put("/api/inventory").contentType(MediaType.APPLICATION_JSON)
                .content("{\"productId\":200001,\"quantity\":4}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/api/inventory/reserve").contentType(MediaType.APPLICATION_JSON).content(record))
                .andExpect(status().isConflict());
        mvc.perform(get("/api/inventory/200001")).andExpect(status().isOk())
                .andExpect(jsonPath("$.quantity").value(5)).andExpect(jsonPath("$.reservedQuantity").value(5))
                .andExpect(jsonPath("$.availableQuantity").value(0));
        mvc.perform(put("/api/inventory").contentType(MediaType.APPLICATION_JSON)
                .content("{\"productId\":200001,\"quantity\":7}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.reservedQuantity").value(5))
                .andExpect(jsonPath("$.availableQuantity").value(2));
    }

    @Test
    void shouldHandleMissingRecordsAndInvalidInputs() throws Exception {
        mvc.perform(get("/api/inventory/9223372036854775807")).andExpect(status().isNotFound());
        mvc.perform(put("/api/inventory").contentType(MediaType.APPLICATION_JSON)
                .content("{\"productId\":9223372036854775807,\"quantity\":1}"))
                .andExpect(status().isNotFound());
        mvc.perform(post("/api/inventory/reserve").contentType(MediaType.APPLICATION_JSON)
                .content("{\"productId\":9223372036854775807,\"quantity\":1}"))
                .andExpect(status().isNotFound());
        mvc.perform(post("/api/inventory/reserve").contentType(MediaType.APPLICATION_JSON)
                .content("{\"productId\":200001,\"quantity\":0}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.validationErrors").isMap());
        mvc.perform(get("/api/inventory/invalid")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
        mvc.perform(post("/api/inventory").contentType(MediaType.APPLICATION_JSON).content("{"))
                .andExpect(status().isBadRequest());
    }
}
