package pl.allegro.tech.hermes.domain.topic.schema;

import pl.allegro.tech.hermes.api.SchemaSource;
import pl.allegro.tech.hermes.api.Topic;

import java.util.List;
import java.util.Optional;

public interface SchemaSourceClient {

    Optional<SchemaSource> getSchemaSource(Topic topic, SchemaVersion version);

    Optional<SchemaSource> getLatestSchemaSource(Topic topic);

    List<SchemaVersion> getVersions(Topic topic);

    void registerSchemaSource(Topic topic, SchemaSource schemaSource);

    void deleteAllSchemaSources(Topic topic);
}
