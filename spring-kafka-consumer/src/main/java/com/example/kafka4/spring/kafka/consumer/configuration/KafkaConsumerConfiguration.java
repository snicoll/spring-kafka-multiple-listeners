package com.example.kafka4.spring.kafka.consumer.configuration;

import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Collections;

/**
 * Additional listener container factories. Each factory uses the settings of a named container,
 * defined using {@code spring.kafka.containers.<name>}, on top of the same configuration as the
 * auto-configured one ({@code spring.kafka.*}, connection details, metrics).
 */
@Configuration(proxyBeanMethods = false)
class KafkaConsumerConfiguration {

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> avroKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            DefaultKafkaConsumerFactoryConfigurer consumerFactoryConfigurer) {
        return createListenerContainerFactory(configurer, consumerFactoryConfigurer, "avro");
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> jsonKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            DefaultKafkaConsumerFactoryConfigurer consumerFactoryConfigurer) {
        return createListenerContainerFactory(configurer, consumerFactoryConfigurer, "json");
    }

    private static ConcurrentKafkaListenerContainerFactory<Object, Object> createListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            DefaultKafkaConsumerFactoryConfigurer consumerFactoryConfigurer, String containerName) {
        DefaultKafkaConsumerFactory<Object, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(Collections.emptyMap());
        consumerFactoryConfigurer.configure(consumerFactory, containerName);
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory, containerName);
        return factory;
    }

}
