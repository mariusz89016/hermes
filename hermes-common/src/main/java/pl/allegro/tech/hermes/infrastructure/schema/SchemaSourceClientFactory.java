package pl.allegro.tech.hermes.infrastructure.schema;

import org.glassfish.hk2.api.Factory;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.domain.topic.schema.SchemaSourceClient;

import javax.inject.Inject;
import javax.ws.rs.client.ClientBuilder;
import java.net.URI;

public class SchemaSourceClientFactory implements Factory<SchemaSourceClient> {

    private final ConfigFactory configFactory;

    @Inject
    public SchemaSourceClientFactory(ConfigFactory configFactory) {
        this.configFactory = configFactory;
    }

    @Override
    public SchemaSourceClient provide() {
        ClientConfig config = new ClientConfig()
                .property(ClientProperties.READ_TIMEOUT, configFactory.getIntProperty(Configs.SCHEMA_REPOSITORY_HTTP_READ_TIMEOUT_MS))
                .property(ClientProperties.CONNECT_TIMEOUT, configFactory.getIntProperty(Configs.SCHEMA_REPOSITORY_HTTP_CONNECT_TIMEOUT_MS));

        return new SchemaRepoSchemaSourceClient(ClientBuilder.newClient(config), URI.create(configFactory.getStringProperty(Configs.SCHEMA_REPOSITORY_SERVER_URL)));
    }

    @Override
    public void dispose(SchemaSourceClient instance) {

    }
}
