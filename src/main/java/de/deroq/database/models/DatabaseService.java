package de.deroq.database.models;

public abstract class DatabaseService {

    protected final DatabaseServiceType databaseServiceType;
    protected String host, username, database, password;
    protected int port;

    public DatabaseService(DatabaseServiceType databaseServiceType, String host, String username, String database, String password, int port) {
        this.databaseServiceType = databaseServiceType;
        this.host = host;
        this.username = username;
        this.database = database;
        this.password = password;
        this.port = port;
    }

    public abstract void connect();

    public abstract void disconnect();

    public DatabaseService(DatabaseServiceType databaseServiceType) {
        this.databaseServiceType = databaseServiceType;
    }

    public DatabaseServiceType getDatabaseServiceType() {
        return databaseServiceType;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
