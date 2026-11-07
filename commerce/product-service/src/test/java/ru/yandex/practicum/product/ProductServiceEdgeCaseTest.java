package ru.yandex.practicum.product;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class ProductServiceEdgeCaseTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper json;

    @Test
    void shouldPreservePatchFieldsAndHideInactiveProductsFromEveryList() throws Exception {
        long categoryId = read(send(post("/api/categories"), "{\"name\":\"Edge category\"}")
                .andExpect(status().isCreated())).get("id").asLong();
        long id = read(send(post("/api/products"), """
                {"name":"UniqueEdgeLamp", "description":"original", "price":12.3456,
                 "imageUrl":"https://example.com/image", "categoryId":%d}
                """.formatted(categoryId)).andExpect(status().isCreated())).get("id").asLong();
        send(patch("/api/products/" + id), "{\"description\":null,\"active\":false}")
                .andExpect(status().isOk()).andExpect(jsonPath("$.name").value("UniqueEdgeLamp"))
                .andExpect(jsonPath("$.description").value("original"))
                .andExpect(jsonPath("$.category.id").value(categoryId));
        JsonNode stored = read(mvc.perform(get("/api/products/" + id)).andExpect(status().isOk()));
        assertThat(stored.get("price").decimalValue()).isEqualByComparingTo("12.3456");
        for (String path : new String[]{"/api/products", "/api/products/category/" + categoryId,
                "/api/products/search?query=uniqueedgelamp"}) {
            JsonNode list = read(mvc.perform(get(path)).andExpect(status().isOk()));
            for (JsonNode product : list) assertThat(product.get("id").asLong()).isNotEqualTo(id);
        }
        send(patch("/api/products/" + id), "{\"active\":true}").andExpect(status().isOk());
        mvc.perform(get("/api/products/search").param("query", "uniqueedgelamp"))
                .andExpect(status().isOk()).andExpect(jsonPath("$[0].id").value(id));
        send(patch("/api/products/" + id), "{}").andExpect(status().isOk())
                .andExpect(jsonPath("$.active").value(true));
    }

    @Test
    void shouldHandleMissingResourcesAndInvalidHttpInputs() throws Exception {
        mvc.perform(get("/api/products/9223372036854775807")).andExpect(status().isNotFound());
        mvc.perform(get("/api/categories/9223372036854775807")).andExpect(status().isNotFound());
        mvc.perform(get("/api/products/category/9223372036854775807")).andExpect(status().isNotFound());
        send(post("/api/products"), "{\"name\":\"Missing category\",\"price\":1,\"categoryId\":9223372036854775807}")
                .andExpect(status().isNotFound());
        send(post("/api/products"), "{\"name\":\"No category\",\"price\":1}")
                .andExpect(status().isCreated()).andExpect(jsonPath("$.category").isEmpty());
        for (MockHttpServletRequestBuilder request : new MockHttpServletRequestBuilder[]{
                get("/api/products/search"), get("/api/products/not-a-number"),
                post("/api/products").contentType(MediaType.APPLICATION_JSON).content("{")}) {
            mvc.perform(request).andExpect(status().isBadRequest()).andExpect(jsonPath("$.status").value(400))
                    .andExpect(jsonPath("$.message").isString()).andExpect(jsonPath("$.timestamp").exists());
        }
        long category = read(send(post("/api/categories"), "{\"name\":\"Empty category\"}"))
                .get("id").asLong();
        mvc.perform(get("/api/products/category/" + category)).andExpect(content().json("[]"));
    }

    private ResultActions send(MockHttpServletRequestBuilder request, String body) throws Exception {
        return mvc.perform(request.contentType(MediaType.APPLICATION_JSON).content(body));
    }

    private JsonNode read(ResultActions response) throws Exception {
        return json.readTree(response.andReturn().getResponse().getContentAsString());
    }
}
