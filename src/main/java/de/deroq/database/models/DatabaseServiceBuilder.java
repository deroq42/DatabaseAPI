package de.deroq.database.models;

import de.deroq.database.services.cassandra.CassandraDatabaseService;
import de.deroq.database.services.mongo.MongoDatabaseService;

import java.util.List;

public class DatabaseServiceBuilder {

    private final DatabaseService databaseService;

    public DatabaseServiceBuilder(DatabaseServiceType databaseServiceType) {
        if(databaseServiceType == DatabaseServiceType.MONGO) {
            this.databaseService = new MongoDatabaseService();
        }else if(databaseServiceType == DatabaseServiceType.CASSANDRA) {
            this.databaseService = new CassandraDatabaseService();
        } else {
            //Fallback database.
            this.databaseService = new MongoDatabaseService();
        }
    }

    public DatabaseServiceBuilder setHost(String host) {
        databaseService.setHost(host);
        return this;
    }

    public DatabaseServiceBuilder setUsername(String username) {
        databaseService.setUsername(username);
        return this;
    }

    public DatabaseServiceBuilder setDatabase(String database) {
        databaseService.setDatabase(database);
        return this;
    }

    public DatabaseServiceBuilder setPassword(String password) {
        databaseService.setPassword(password);
        return this;
    }

    public DatabaseServiceBuilder setPort(int port) {
        databaseService.setPort(port);
        return this;
    }

    public DatabaseServiceBuilder setKeySpace(String keySpace) {
        databaseService.setKeySpace(keySpace);
        return this;
    }

    public DatabaseServiceBuilder setMappers(List<Class<?>> mappers) {
        databaseService.setMappers(mappers);
        return this;
    }

    public DatabaseService build() {
        return databaseService;
    }
}
