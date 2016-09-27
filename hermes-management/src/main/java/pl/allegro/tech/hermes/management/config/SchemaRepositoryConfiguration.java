package pl.allegro.tech.hermes.management.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.main.JsonSchema;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import pl.allegro.tech.hermes.domain.topic.schema.CompiledSchemaRepository;
import pl.allegro.tech.hermes.domain.topic.schema.DirectCompiledSchemaRepository;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaCompilersFactory;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaRepository;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaSourceClient;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaVersionsRepository;
import pl.allegro.tech.hermes.domain.topic.schema.SimpleSchemaVersionsRepository;
import pl.allegro.tech.hermes.infrastructure.schema.SchemaRepoSchemaSourceClient;
import pl.allegro.tech.hermes.management.domain.topic.TopicService;

import javax.ws.rs.client.Client;
import java.net.URI;

@Configuration
@EnableConfigurationProperties({SchemaRepositoryProperties.class})
public class SchemaRepositoryConfiguration {

    @Autowired
    @Lazy
    TopicService topicService;

    @Autowired
    private SchemaRepositoryProperties schemaRepositoryProperties;

    @Bean
    @ConditionalOnMissingBean(SchemaSourceClient.class)
    @ConditionalOnProperty(value = "schema.repository.type", havingValue = "schema_repo")
    public SchemaSourceClient schemaRepoSchemaSourceClient(Client httpClient) {
        return new SchemaRepoSchemaSourceClient(httpClient, URI.create(schemaRepositoryProperties.getServerUrl()));
    }

    @Bean
    public SchemaRepository aggregateSchemaRepository(SchemaSourceClient schemaSourceClient, ObjectMapper objectMapper) {
        SchemaVersionsRepository versionsRepository = new SimpleSchemaVersionsRepository(schemaSourceClient);
        CompiledSchemaRepository<Schema> avroSchemaRepository = new DirectCompiledSchemaRepository<>(
                schemaSourceClient, SchemaCompilersFactory.avroSchemaCompiler());
        CompiledSchemaRepository<JsonSchema> jsonSchemaRepository = new DirectCompiledSchemaRepository<>(
                schemaSourceClient, SchemaCompilersFactory.jsonSchemaCompiler(objectMapper));

        return new SchemaRepository(versionsRepository, avroSchemaRepository, jsonSchemaRepository);
    }

}
