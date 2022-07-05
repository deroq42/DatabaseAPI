package de.deroq.database.services.cassandra;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CassandraDatabaseServiceMethods extends CassandraDatabaseService {

    protected final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    public void createTable(String keyspace, String name, String values) {
        CompletableFuture.runAsync(() -> session.executeAsync("CREATE TABLE IF NOT EXISTS " + keyspace + "." + name + "(" + values + ");"), EXECUTOR_SERVICE);
    }

    public <T> CompletableFuture<Boolean> onInsert(T entity, Class<T> aClass) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> getMapper(aClass).save(entity), EXECUTOR_SERVICE);
        return future;
    }

    public <T> CompletableFuture<Boolean> onDelete(T entity, Class<T> aClass) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> getMapper(aClass).delete(entity), EXECUTOR_SERVICE);
        return future;
    }

    public <T> CompletableFuture<Boolean> onUpdate(T entity, Class<T> aClass) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> getMapper(aClass).save(entity), EXECUTOR_SERVICE);
        return future;
    }

    public <T> CompletableFuture<T> getAsync(String key, Class<T> aClass) {
        CompletableFuture<T> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            T entity = getMapper(aClass).get(key);
            future.complete(entity);
        }, EXECUTOR_SERVICE);

        return future;
    }
}
