package de.deroq.database.services.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import de.deroq.database.models.DatabaseService;
import de.deroq.database.models.DatabaseServiceType;

import java.util.List;

public class CassandraDatabaseService extends DatabaseService {

    public CassandraDatabaseServiceMethods databaseServiceMethods;
    private Cluster cluster;
    private Session session;
    private MappingManager mappingManager;

    public CassandraDatabaseService(String host, String username, String database, String password, int port, String keySpace, List<Class<?>> mappers) {
        super(DatabaseServiceType.CASSANDRA, host, username, database, password, port, keySpace, mappers);
        this.databaseServiceMethods = new CassandraDatabaseServiceMethods(this);
    }

    @Override
    public void connect() {
        /* Creates a connection pool with the following options. */
        PoolingOptions poolingOptions = new PoolingOptions()
                .setConnectionsPerHost(HostDistance.LOCAL, 4, 10)
                .setConnectionsPerHost(HostDistance.REMOTE, 2, 4)
                .setHeartbeatIntervalSeconds(60);

        /* Creates a cluster with the above set pooling options. */
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
