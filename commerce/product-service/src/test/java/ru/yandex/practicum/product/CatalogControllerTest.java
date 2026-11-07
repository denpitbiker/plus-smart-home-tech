package ru.yandex.practicum.product;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import ru.yandex.practicum.product.controller.CategoryController;
import ru.yandex.practicum.product.controller.ProductController;
import ru.yandex.practicum.product.dto.UpdateProductRequest;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.service.CatalogService;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest({ProductController.class, CategoryController.class})
class CatalogControllerTest {
    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper json;
    @MockBean private CatalogService service;

    @Test
    void shouldReturnCreatedProductAndCategory() throws Exception {
        when(service.createCategory(Stubs.categoryRequest())).thenReturn(Stubs.category());
        when(service.createProduct(Stubs.productRequest())).thenReturn(Stubs.product());

        mvc.perform(post("/api/categories").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Stubs.categoryRequest())))
                .andExpect(status().isCreated())
                .andExpect(content().json(json.writeValueAsString(Stubs.category())));
        mvc.perform(post("/api/products").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Stubs.productRequest())))
                .andExpect(status().isCreated())
                .andExpect(content().json(json.writeValueAsString(Stubs.product())));
        verify(service).createCategory(Stubs.categoryRequest());
        verify(service).createProduct(Stubs.productRequest());
    }

    @Test
    void shouldRouteReadsToService() throws Exception {
        when(service.getProducts()).thenReturn(List.of(Stubs.product()));
        when(service.getProduct(Stubs.PRODUCT_ID)).thenReturn(Stubs.product());
        when(service.getProductsByCategory(Stubs.CATEGORY_ID)).thenReturn(List.of(Stubs.product()));
        when(service.search("Лампа")).thenReturn(List.of(Stubs.product()));
        when(service.getCategories()).thenReturn(List.of(Stubs.category()));
        when(service.getCategory(Stubs.CATEGORY_ID)).thenReturn(Stubs.category());

        mvc.perform(get("/api/products")).andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(Stubs.PRODUCT_ID));
        mvc.perform(get("/api/products/{id}", Stubs.PRODUCT_ID)).andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(Stubs.PRODUCT_ID));
        mvc.perform(get("/api/products/category/{id}", Stubs.CATEGORY_ID)).andExpect(status().isOk())
                .andExpect(jsonPath("$[0].category.id").value(Stubs.CATEGORY_ID));
        mvc.perform(get("/api/products/search").param("query", "Лампа")).andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(Stubs.PRODUCT_ID));
        mvc.perform(get("/api/categories")).andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value(Stubs.CATEGORY_ID));
        mvc.perform(get("/api/categories/{id}", Stubs.CATEGORY_ID)).andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(Stubs.CATEGORY_ID));
        verify(service).search("Лампа");
    }

    @Test
    void shouldPassPartialUpdateToService() throws Exception {
        UpdateProductRequest update = new UpdateProductRequest(null, null, null, null, null, false);
        when(service.updateProduct(Stubs.PRODUCT_ID, update)).thenReturn(Stubs.product(false));
        mvc.perform(patch("/api/products/{id}", Stubs.PRODUCT_ID).contentType(MediaType.APPLICATION_JSON)
                        .content("{\"active\":false}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.active").value(false));
        verify(service).updateProduct(Stubs.PRODUCT_ID, update);
    }

    @Test
    void shouldRejectInvalidRequestsBeforeCallingService() throws Exception {
        mvc.perform(post("/api/products").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.validationErrors.name").exists())
                .andExpect(jsonPath("$.validationErrors.price").exists());
        mvc.perform(post("/api/categories").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.validationErrors.name").exists());
        mvc.perform(get("/api/products/search")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/products/invalid")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/products").contentType(MediaType.APPLICATION_JSON).content("{"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void shouldUseErrorHandlerForMissingResources() throws Exception {
        when(service.getProduct(Stubs.PRODUCT_ID)).thenThrow(new NotFoundException("Товар не найден"));
        when(service.createProduct(any())).thenThrow(new NotFoundException("Категория не найдена"));
        mvc.perform(get("/api/products/{id}", Stubs.PRODUCT_ID)).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message").value("Товар не найден"))
                .andExpect(jsonPath("$.timestamp").exists());
        mvc.perform(post("/api/products").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Stubs.productRequest())))
                .andExpect(status().isNotFound()).andExpect(jsonPath("$.message").value("Категория не найдена"));
    }
}
