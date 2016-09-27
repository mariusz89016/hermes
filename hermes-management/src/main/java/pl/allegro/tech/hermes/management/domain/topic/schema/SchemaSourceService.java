package pl.allegro.tech.hermes.management.domain.topic.schema;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import pl.allegro.tech.hermes.api.SchemaSource;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaSourceClient;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaVersion;
import pl.allegro.tech.hermes.management.domain.topic.TopicService;
import pl.allegro.tech.hermes.management.infrastructure.schema.validator.SchemaValidator;
import pl.allegro.tech.hermes.management.infrastructure.schema.validator.SchemaValidatorProvider;

import java.util.Optional;

import static pl.allegro.tech.hermes.api.ContentType.AVRO;
import static pl.allegro.tech.hermes.api.TopicName.fromQualifiedName;

@Component
public class SchemaSourceService {

    private final TopicService topicService;
    private final SchemaSourceClient schemaSourceClient;
    private final SchemaValidatorProvider validatorProvider;

    @Autowired
    public SchemaSourceService(TopicService topicService, SchemaSourceClient schemaSourceClient, SchemaValidatorProvider validatorProvider) {
        this.topicService = topicService;
        this.schemaSourceClient = schemaSourceClient;
        this.validatorProvider = validatorProvider;
    }

    public Optional<SchemaSource> getSchemaSource(String qualifiedTopicName) {
        Topic topic = findTopic(qualifiedTopicName);
        return schemaSourceClient.getLatestSchemaSource(topic);
    }

    public void registerSchemaSource(String qualifiedTopicName, String schema, boolean validate) {
        Topic topic = findTopic(qualifiedTopicName);
        if (validate) {
            SchemaValidator validator = validatorProvider.provide(topic.getContentType());
            validator.check(schema);
        }
        schemaSourceClient.registerSchemaSource(topic, SchemaSource.valueOf(schema));
    }

    public Optional<SchemaSource> getSchemaSource(String qualifiedTopicName, SchemaVersion version) {
        Topic topic = findTopic(qualifiedTopicName);
        return schemaSourceClient.getSchemaSource(topic, version);
    }

    public void deleteAllSchemaSources(String qualifiedTopicName) {
        Topic topic = findTopic(qualifiedTopicName);
        // TODO wywalić ifa?
        if (topic.getContentType() == AVRO) {
            throw new AvroSchemaRemovalDisabledException("Topic " + qualifiedTopicName + " has Avro content-type, schema removal is disabled");
        }
        schemaSourceClient.deleteAllSchemaSources(topic);
    }

    private Topic findTopic(String qualifiedTopicName) {
        return topicService.getTopicDetails(fromQualifiedName(qualifiedTopicName));
    }
}
