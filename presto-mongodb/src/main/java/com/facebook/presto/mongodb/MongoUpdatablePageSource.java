/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.mongodb;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.Block;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOptions;
import io.airlift.slice.Slice;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MongoUpdatablePageSource implements UpdatablePageSource {
    private final MongoSession mongoSession;
    private final MongoSplit split;
    private final MongoPageSource inner;

    public MongoUpdatablePageSource(MongoSession mongoSession, MongoSplit split, List<MongoColumnHandle> columns) {
        this.mongoSession = mongoSession;
        this.split = split;
        this.inner = new MongoPageSource(mongoSession, split, columns);
    }

    @Override
    public void deleteRows(Block rowIds) {
        //row-level delete
        MongoCollection<Document> collection = mongoSession.getCollection(split.getSchemaTableName());
        BasicDBObject filter  = new BasicDBObject();
        //filter.append();
        DeleteOptions op = new DeleteOptions();

        try{
            for(int i = 0;i < rowIds.getPositionCount();i++){
               if(!rowIds.isNull(i)){
                   Slice slice = rowIds.getSlice(i, 0, rowIds.getSliceLength(i));
                   filter.put("_id",new BasicDBObject("$eq", new ObjectId(slice.getBytes())));
                   collection.deleteOne(filter,op);
               }
            }
        }catch (MongoException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        CompletableFuture<Collection<Slice>> cf = new CompletableFuture<>();
        cf.complete(Collections.emptyList());
        return cf;
    }

    @Override
    public void abort() {

    }

    @Override
    public long getCompletedBytes() {
        return inner.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions() {
        return inner.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos() {
        return inner.getReadTimeNanos();
    }

    @Override
    public boolean isFinished() {
        return inner.isFinished();
    }

    @Override
    public Page getNextPage() {
        return inner.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage() {
        return inner.getSystemMemoryUsage();
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }
}
