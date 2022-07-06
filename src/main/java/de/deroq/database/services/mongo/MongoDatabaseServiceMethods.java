package de.deroq.database.services.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MongoDatabaseServiceMethods extends MongoDatabaseService {

    protected final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    /**
     * Inserts an object into the database asynchronously.
     *
     * @param collection The collection where to insert the object.
     * @param filter The filter to modify the query and get the result.
     * @param t The object to insert.
     * @return a Future with a Boolean which returns false if the object has been inserted.
     * @param <T> The specific type of the object.
     */
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

    /***
     * Deletes an object from the database asynchronously.
     *
     * @param collection The collection where to delete the object.
     * @param filter The filter to modify the query and get the result.
     * @return a Future with a Boolean which returns false if the object has been deleted.
     * @param <T> The specific type of the object.
     */
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

    /**
     * Updates an object in the database asynchronously.
     *
     * @param collection The collection where to update the object.
     * @param filter The filter to modify the query and get the result.
     * @param t The object to update.
     * @return a Future with a Boolean which returns false if the object has been updated.
     * @param <T> The specific type of the object.
     */
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

    /**
     * Gets an object from the database asynchronously.
     *
     * @param collection The collection where to get the object.
     * @param filter The filter to modify the query and get the result.
     * @return a Future with the object. Returns null if it could not be found.
     * @param <T> The specific type of the object.
     */
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


    /**
     * Gets a list of objects from the database asynchronously.
     *
     * @param collection The collection where to get the objects.
     * @return a Future with a Collection of the objects.
     * @param <T> The specific type of the object.
     */
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
