package de.deroq.database.services.cassandra;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CassandraDatabaseServiceMethods {

    private final CassandraDatabaseService databaseService;
    protected final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    public CassandraDatabaseServiceMethods(CassandraDatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    /**
     * Creates a table in the database asynchronously.
     *
     * @param keyspace The keyspace of the table.
     * @param name The name of the table.
     * @param columns The columns of the table to insert entities.
     */
    public void createTable(String keyspace, String name, String columns) {
        CompletableFuture.runAsync(() -> databaseService.getSession().executeAsync("CREATE TABLE IF NOT EXISTS " + keyspace + "." + name + "(" + columns + ");"), EXECUTOR_SERVICE);
    }

    /**
     * Inserts an entity into the database asynchronously.
     *
     * @param entity The entity we want to insert into the database.
     * @param aClass The class of the entity.
     * @param <T> The specified type of the entity.
     */
    public <T> void onInsert(T entity, Class<T> aClass) {
        CompletableFuture.runAsync(() -> databaseService.getMapper(aClass).save(entity), EXECUTOR_SERVICE);
    }

    /**
     * Deletes an entity from the database asynchronously.
     *
     * @param entity The entity we want to delete from the database.
     * @param aClass The class of the entity.
     * @param <T> The specified type of the entity.
     */
    public <T> void onDelete(T entity, Class<T> aClass) {
        CompletableFuture.runAsync(() -> databaseService.getMapper(aClass).delete(entity), EXECUTOR_SERVICE);
    }

    /**
     * Updates an entity in the database asynchronously.
     *
     * @param entity The entity we want to update in the database.
     * @param aClass The class of the entity.
     * @param <T> The specified type of the entity.
     */
    public <T> void onUpdate(T entity, Class<T> aClass) {
        CompletableFuture.runAsync(() -> databaseService.getMapper(aClass).save(entity), EXECUTOR_SERVICE);
    }

    /**
     * Gets an object from the database asynchronously.
     *
     * @param key The primary key to get the entity.
     * @param aClass The class of the entity.
     * @return a Future with an entity of the type T
     * @param <T> The specified type of the entity.
     */
    public <T> CompletableFuture<T> getAsync(String key, Class<T> aClass) {
        CompletableFuture<T> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            T entity = databaseService.getMapper(aClass).get(key);
            future.complete(entity);
        }, EXECUTOR_SERVICE);

        return future;
    }
}
