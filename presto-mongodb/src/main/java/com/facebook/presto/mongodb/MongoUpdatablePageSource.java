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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.Block;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOptions;
import io.airlift.slice.Slice;
import org.bson.Document;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class MongoUpdatablePageSource implements UpdatablePageSource {
    private final MongoSession mongoSession;
    private final SchemaTableName schemaTableName;

    public MongoUpdatablePageSource(MongoSession mongoSession,SchemaTableName schemaTableName) {
        this.mongoSession = mongoSession;
        this.schemaTableName = schemaTableName;
    }

    @Override
    public void deleteRows(Block rowIds) {
        //row-level delete
        MongoCollection<Document> collection = mongoSession.getCollection(schemaTableName);
        BasicDBObject filter  = new BasicDBObject();
        //filter.append();

        DeleteOptions op = new DeleteOptions();
        //fragments.stream().map(Sl)
        try{
            for(int i = 0;i < rowIds.getPositionCount();i++){
               if(!rowIds.isNull(i)){
                   filter.put("_id",new BasicDBObject("$eq",rowIds.getSlice(i,0,rowIds.getSliceLength(i))));
                   collection.deleteOne(filter,op);
               }
            }
        }catch (MongoException e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return null;
    }

    @Override
    public void abort() {

    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getCompletedPositions() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public Page getNextPage() {
        return null;
    }

    @Override
    public long getSystemMemoryUsage() {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
