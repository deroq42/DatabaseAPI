package de.deroq.database.services.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.deroq.database.models.DatabaseService;
import de.deroq.database.models.DatabaseServiceType;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoDatabaseService extends DatabaseService {

    public final MongoDatabaseServiceMethods databaseServiceMethods;
    protected MongoClient mongoClient;
    protected MongoDatabase mongoDatabase;

    public MongoDatabaseService(String host, String username, String database, String password, int port) {
        super(DatabaseServiceType.MONGO, host, username, database, password, port);
        this.databaseServiceMethods = new MongoDatabaseServiceMethods();
    }

    @Override
    public void connect() {
        /* URL of the mongo connection. */
        ConnectionString connectionString = new ConnectionString("mongodb://" + host + ":" + port);
        CodecRegistry pojoCodecRegistry = fromProviders(
                PojoCodecProvider.builder()
                        .automatic(true)
                        .build());

        CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);
        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .codecRegistry(codecRegistry)
                .build();

        this.mongoClient = MongoClients.create(clientSettings);
        this.mongoDatabase = mongoClient.getDatabase(database);
    }

    @Override
    public void disconnect() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    public <T> MongoCollection<T> getCollection(String name, Class<T> clazz) {
        return mongoDatabase.getCollection(name, clazz);
    }

    public MongoDatabaseServiceMethods getDatabaseServiceMethods() {
        return databaseServiceMethods;
    }

    protected MongoClient getMongoClient() {
        return mongoClient;
    }

    protected com.mongodb.client.MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }
}
