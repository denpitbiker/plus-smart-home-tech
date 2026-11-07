package ru.yandex.practicum.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class OrderServiceEdgeCaseTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper json;

    @Test
    void shouldPersistExactTotalAndIndependentSnapshots() throws Exception {
        String body = """
                {"customerName":"Buyer", "customerEmail":"snapshot@example.com", "items":[
                  {"productId":900001, "productName":"Original", "quantity":3, "price":0.1234},
                  {"productId":900002, "productName":"Second", "quantity":2, "price":1.01}]}
                """;
        JsonNode created = json.readTree(mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                        .content(body)).andExpect(status().isCreated()).andReturn().getResponse().getContentAsString());
        long id = created.get("id").asLong();
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                .content(body.replace("Original", "Changed").replace("0.1234", "999.99")))
                .andExpect(status().isCreated());
        JsonNode stored = json.readTree(mvc.perform(get("/api/orders/" + id)).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.createdAt").isString()).andExpect(jsonPath("$.statusDetails").isEmpty())
                .andReturn().getResponse().getContentAsString());
        assertThat(stored.get("totalPrice").decimalValue()).isEqualByComparingTo("2.3902");
        assertThat(stored.get("items").get(0).get("price").decimalValue()).isEqualByComparingTo("0.1234");
        assertThat(stored.get("items").get(0).get("productName").asText()).isEqualTo("Original");
        mvc.perform(get("/api/orders/by-email").param("email", "nobody@example.com"))
                .andExpect(status().isOk()).andExpect(content().json("[]"));
    }

    @Test
    void shouldRejectInvalidNestedItemsAndMalformedRequests() throws Exception {
        for (String items : new String[]{"[null]", "[]", "null", "[{\"productId\":1,\"quantity\":1}]",
                "[{\"productId\":1,\"productName\":\"\",\"quantity\":0,\"price\":0}]"}) {
            mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON)
                    .content("{\"customerName\":\"Buyer\",\"customerEmail\":\"buyer@example.com\",\"items\":" + items + "}"))
                    .andExpect(status().isBadRequest()).andExpect(jsonPath("$.validationErrors").isMap());
        }
        mvc.perform(get("/api/orders/9223372036854775807")).andExpect(status().isNotFound());
        mvc.perform(get("/api/orders/by-email")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
        mvc.perform(get("/api/orders/invalid")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/orders").contentType(MediaType.APPLICATION_JSON).content("{"))
                .andExpect(status().isBadRequest());
    }
}
