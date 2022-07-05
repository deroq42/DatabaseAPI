package de.deroq.database.services.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import de.deroq.database.models.DatabaseService;
import de.deroq.database.models.DatabaseServiceType;

public class CassandraDatabaseService extends DatabaseService {

    public CassandraDatabaseServiceMethods databaseServiceMethods;
    protected Cluster cluster;
    protected Session session;
    protected MappingManager mappingManager;

    public CassandraDatabaseService() {
        super(DatabaseServiceType.CASSANDRA);
        this.databaseServiceMethods = new CassandraDatabaseServiceMethods();
    }

    @Override
    public void connect() {
        PoolingOptions poolingOptions = new PoolingOptions()
                .setConnectionsPerHost(HostDistance.LOCAL, 4, 10)
                .setConnectionsPerHost(HostDistance.REMOTE, 2, 4)
                .setHeartbeatIntervalSeconds(60);

        this.cluster = Cluster.builder()
                .addContactPoint(host)
                .withPoolingOptions(poolingOptions)
                .build();

        this.session = cluster.connect(keySpace);
        this.mappingManager = new MappingManager(session);
    }

    @Override
    public void disconnect() {
        if(cluster != null) {
            cluster.close();
        }
    }

    public CassandraDatabaseServiceMethods getDatabaseServiceMethods() {
        return databaseServiceMethods;
    }

    protected Cluster getCluster() {
        return cluster;
    }

    protected Session getSession() {
        return session;
    }

    protected MappingManager getMappingManager() {
        return mappingManager;
    }

    public <T> Mapper<T> getMapper(Class<T> aClass) {
        return mappingManager.mapper(aClass);
    }
}
