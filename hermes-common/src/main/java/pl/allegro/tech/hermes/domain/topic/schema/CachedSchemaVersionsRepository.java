package pl.allegro.tech.hermes.domain.topic.schema;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Topic;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

public class CachedSchemaVersionsRepository implements SchemaVersionsRepository {

    private static final Logger logger = LoggerFactory.getLogger(CachedSchemaVersionsRepository.class);

    private final SchemaSourceClient schemaSourceClient;
    private final LoadingCache<Topic, List<SchemaVersion>> versionsCache;

    public CachedSchemaVersionsRepository(SchemaSourceClient schemaSourceClient, ExecutorService versionsReloader,
                                          int refreshAfterWriteMinutes, int expireAfterWriteMinutes) {
        this(schemaSourceClient, versionsReloader, refreshAfterWriteMinutes, expireAfterWriteMinutes, Ticker.systemTicker());
    }

    CachedSchemaVersionsRepository(SchemaSourceClient schemaSourceClient, ExecutorService versionsReloader,
                                   int refreshAfterWriteMinutes, int expireAfterWriteMinutes, Ticker ticker) {
        this.schemaSourceClient = schemaSourceClient;
        this.versionsCache = CacheBuilder
                .newBuilder()
                .ticker(ticker)
                .refreshAfterWrite(refreshAfterWriteMinutes, TimeUnit.MINUTES)
                .expireAfterWrite(expireAfterWriteMinutes, TimeUnit.MINUTES)
                .build(new SchemaVersionsLoader(schemaSourceClient, versionsReloader));
    }

    @Override
    public List<SchemaVersion> versions(Topic topic, boolean online) {
        try {
            return online? schemaSourceClient.getVersions(topic) : versionsCache.get(topic);
        } catch (ExecutionException e) {
            logger.error("Error while loading schema versions for topic {}", topic.getQualifiedName(), e);
            return emptyList();
        }
    }

    private static class SchemaVersionsLoader extends CacheLoader<Topic, List<SchemaVersion>> {

        private final SchemaSourceClient schemaSourceClient;
        private final ExecutorService versionsReloader;

        public SchemaVersionsLoader(SchemaSourceClient schemaSourceClient, ExecutorService versionsReloader) {
            this.schemaSourceClient = schemaSourceClient;
            this.versionsReloader = versionsReloader;
        }

        @Override
        public List<SchemaVersion> load(Topic topic) throws Exception {
            logger.info("Loading schema versions for topic {}", topic.getQualifiedName());
            return schemaSourceClient.getVersions(topic);
        }

        @Override
        public ListenableFuture<List<SchemaVersion>> reload(Topic topic, List<SchemaVersion> oldVersions) throws Exception {
            ListenableFutureTask<List<SchemaVersion>> task = ListenableFutureTask.create(() -> {
                logger.info("Reloading schema versions for topic {}", topic.getQualifiedName());
                try {
                    return schemaSourceClient.getVersions(topic);
                } catch (Exception e) {
                    logger.warn("Could not reload schema versions for topic {}", topic.getQualifiedName(), e);
                    throw e;
                }
            });
            versionsReloader.execute(task);
            return task;
        }
    }
}
