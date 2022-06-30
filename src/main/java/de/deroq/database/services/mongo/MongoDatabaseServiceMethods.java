package de.deroq.database.services.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MongoDatabaseServiceMethods {

    public final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    public <T> CompletableFuture<Boolean> onInsert(MongoCollection<T> collection, Bson filter, T t) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
                T document = collection.find(filter).first();
                if (document != null) {
                    future.complete(true);
                    return;
                }

                collection.insertOne(t);
                future.complete(false);
        }, EXECUTOR_SERVICE);
        return future;
    }

    public <T> CompletableFuture<Boolean> onDelete(MongoCollection<T> collection, Bson filter) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            T document = collection.find(filter).first();
            if (document == null) {
                future.complete(true);
                return;
            }

            collection.deleteOne(filter);
            future.complete(false);
        }, EXECUTOR_SERVICE);

        return future;
    }

    public <T> CompletableFuture<Boolean> onUpdate(MongoCollection<T> collection, Bson filter, T t) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            T document = collection.find(filter).first();
            if (document == null) {
                future.complete(true);
                return;
            }

            collection.replaceOne(filter, t);
            future.complete(false);
        }, EXECUTOR_SERVICE);

        return future;
    }

    public <T> CompletableFuture<T> getAsync(MongoCollection<T> collection, Bson filter) {
        CompletableFuture<T> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            T document = collection.find(filter).first();
            if (document == null) {
                future.complete(null);
                return;
            }

            future.complete(document);
        }, EXECUTOR_SERVICE);

        return future;
    }

    public <T> CompletableFuture<Collection<T>> getAsyncCollection(MongoCollection<T> collection) {
        CompletableFuture<Collection<T>> future = new CompletableFuture<>();
        Collection<T> list = new ArrayList<>();

        CompletableFuture.runAsync(() -> {
            collection.find().forEach(list::add);
            future.complete(list);
        }, EXECUTOR_SERVICE);

        return future;
    }
}
