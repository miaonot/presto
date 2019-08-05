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
package com.facebook.presto.plugin.hugegraph;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class HugeGraphRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(HugeGraphRecordCursor.class);

    private final HugeGraphColumnHandle[] columnHandles;
    private final ResultSet resultSet;
    private final HugeGraphClient hugeGraphClient;

    private final Client client;

    private Result result;

    private boolean closed;

    public HugeGraphRecordCursor(HugeGraphClient hugeGraphClient, HugeGraphSplit split, List<HugeGraphColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles.toArray(new HugeGraphColumnHandle[0]);
        this.hugeGraphClient = hugeGraphClient;

        client = hugeGraphClient.gremlinClientFactory.openClient();
        log.debug("Executing: %s", split.getGremlinQuery());
        resultSet = client.submit(split.getGremlinQuery());
    }

    @Override
    public long getCompletedBytes()
    {
        checkState(!closed, "cursor is closed");
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        checkState(!closed, "cursor is closed");
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkState(!closed, "cursor is closed");
        return columnHandles[field].getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        checkState(!closed, "cursor is closed");
        result = resultSet.one();
        return !result.isNull();
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        return result.getBoolean();
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        return result.getInt();
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        return result.getDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        return Slices.utf8Slice(result.getString());
    }

    @Override
    public Object getObject(int field)
    {
        checkState(!closed, "cursor is closed");
        return result.getObject();
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        return result.isNull();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }

        closed = true;
    }
}
