package ru.yandex.practicum.collector.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JsonConfiguration {
    @Bean
    Jackson2ObjectMapperBuilderCustomizer strictJsonTypes() {
        return builder -> builder
                .featuresToDisable(MapperFeature.ALLOW_COERCION_OF_SCALARS,
                        DeserializationFeature.ACCEPT_FLOAT_AS_INT)
                .featuresToEnable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
                .postConfigurer(mapper -> {
                    for (var shape : new CoercionInputShape[] {CoercionInputShape.Integer,
                            CoercionInputShape.Float, CoercionInputShape.Boolean}) {
                        mapper.coercionConfigFor(LogicalType.Textual).setCoercion(shape, CoercionAction.Fail);
                        mapper.coercionConfigFor(LogicalType.DateTime).setCoercion(shape, CoercionAction.Fail);
                    }
                });
    }
}
