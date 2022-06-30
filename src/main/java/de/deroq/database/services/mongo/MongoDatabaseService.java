package de.deroq.database.services.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import de.deroq.database.models.DatabaseService;
import de.deroq.database.models.DatabaseServiceType;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class MongoDatabaseService extends DatabaseService {

    private final MongoDatabaseServiceMethods mongoDatabaseServiceMethods;
    private MongoClient mongoClient;
    private com.mongodb.client.MongoDatabase mongoDatabase;

    public MongoDatabaseService() {
        super(DatabaseServiceType.MONGO);
        this.mongoDatabaseServiceMethods = new MongoDatabaseServiceMethods();
    }

    @Override
    public void connect() {
        ConnectionString connectionString = new ConnectionString("mongodb://" + host + ":" + port);
        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
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
        if(mongoClient != null) {
            mongoClient.close();
        }
    }

    public <T> MongoCollection<T> getCollection(String name, Class<T> clazz) {
        return mongoDatabase.getCollection(name, clazz);
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public com.mongodb.client.MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    public MongoDatabaseServiceMethods getMongoDatabaseServiceMethods() {
        return mongoDatabaseServiceMethods;
    }
}
