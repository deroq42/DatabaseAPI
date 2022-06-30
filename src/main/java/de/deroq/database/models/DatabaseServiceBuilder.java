package de.deroq.database.models;

import de.deroq.database.services.mongo.MongoDatabaseService;

public class DatabaseServiceBuilder {

    private final DatabaseService databaseService;

    public DatabaseServiceBuilder(DatabaseServiceType databaseServiceType) {
        if(databaseServiceType == DatabaseServiceType.MONGO) {
            this.databaseService = new MongoDatabaseService();
        } else {
            //Default / Fallback database.
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

    public DatabaseService build() {
        return databaseService;
    }
}
