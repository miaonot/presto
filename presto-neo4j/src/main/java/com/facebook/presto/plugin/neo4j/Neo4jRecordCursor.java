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
package com.facebook.presto.plugin.neo4j;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class Neo4jRecordCursor
        implements RecordCursor
{
    public static final Logger log = Logger.get(Neo4jRecordCursor.class);

    private final Neo4jColumnHandle[] columnHandles;
    private final Neo4jClient neo4jClient;
    private final boolean isPath;

    private final Session session;
    private final Result result;
    private Record record;
    private List<Node> nodes;
    private List<Relationship> relationships;

    private boolean closed;

    public Neo4jRecordCursor(Neo4jClient neo4jClient, Neo4jSplit split, List<Neo4jColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles.toArray(new Neo4jColumnHandle[0]);
        this.neo4jClient = neo4jClient;
        this.isPath = split.isPath();

        String matchStatement = split.getSchemaName();
        String token = null;
        String statement;
        if (matchStatement.contains("=")) {
            String[] splitResult = matchStatement.split("=");
            token = splitResult[0];
        }
        if (token != null) {
            statement = "match " + matchStatement + " return" + token;
        }
        else {
            statement = "match p = " + matchStatement + " return p";
        }

        session = neo4jClient.openSession();
        this.result = session.run(statement);
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
        if (result.hasNext()) {
            record = result.next();

            nodes = new ArrayList<>();
            record.get(0).asPath().nodes().forEach(nodes::add);

            relationships = new ArrayList<>();
            record.get(0).asPath().relationships().forEach(relationships::add);

            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        String columnName = columnHandles[field].getColumnName();
        int num;
        if (columnName.substring(0, 4).equals("node")) {
            num = Integer.parseInt(columnName.substring(4, 5));
            return nodes.get(num).get(columnName.substring(5)).asBoolean();
        }
        else {
            num = Integer.parseInt(columnName.substring(12, 13));
            return relationships.get(num).get(columnName.substring(13)).asBoolean();
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        String columnName = columnHandles[field].getColumnName();
        int num;
        if (columnName.substring(0, 4).equals("node")) {
            num = Integer.parseInt(columnName.substring(4, 5));
            return nodes.get(num).get(columnName.substring(5)).asLong();
        }
        else {
            num = Integer.parseInt(columnName.substring(12, 13));
            return relationships.get(num).get(columnName.substring(13)).asLong();
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        String columnName = columnHandles[field].getColumnName();
        int num;
        if (columnName.substring(0, 4).equals("node")) {
            num = Integer.parseInt(columnName.substring(4, 5));
            return nodes.get(num).get(columnName.substring(5)).asDouble();
        }
        else {
            num = Integer.parseInt(columnName.substring(12, 13));
            return relationships.get(num).get(columnName.substring(13)).asDouble();
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        String columnName = columnHandles[field].getColumnName();
        int type = columnHandles[field].getNeo4jTypeHandle().getType();
        if (!isPath) {
            int num;
            if (type == Types.ARRAY) {
                if (columnName.substring(0, 4).equals("node")) {
                    num = Integer.parseInt(columnName.substring(4, 5));
                    return Slices.utf8Slice(nodes.get(num).get(columnName.substring(5)).asList().toString());
                }
                else {
                    num = Integer.parseInt(columnName.substring(12, 13));
                    return Slices.utf8Slice(relationships.get(num).get(columnName.substring(13)).asList().toString());
                }
            }
            else {
                if (columnName.substring(0, 4).equals("node")) {
                    num = Integer.parseInt(columnName.substring(4, 5));
                    if (columnName.matches("^node[0-9]\\d*$")) {
                        return Slices.utf8Slice(nodes.get(num).asMap().toString());
                    }
                    else {
                        return Slices.utf8Slice(nodes.get(num).get(columnName.substring(5)).asString());
                    }
                }
                else {
                    num = Integer.parseInt(columnName.substring(12, 13));
                    if (columnName.matches("^relationship[0-9]\\d*$")) {
                        return Slices.utf8Slice(relationships.get(num).asMap().toString());
                    }
                    else {
                        return Slices.utf8Slice(relationships.get(num).get(columnName.substring(13)).asString());
                    }
                }
            }
        }
        else {
            StringBuilder builder = new StringBuilder("[");
            Path path = record.get(0).asPath();
            Iterator<Path.Segment> segmentIterator = path.iterator();
            if (segmentIterator.hasNext()) {
                Path.Segment segment = segmentIterator.next();
                builder.append(segment.start().asMap().toString()).append(", ");
                builder.append(segment.relationship().asMap().toString()).append(", ");
                while (segmentIterator.hasNext()) {
                    segment = segmentIterator.next();
                    builder.append(segment.start().asMap().toString()).append(", ");
                    builder.append(segment.relationship().asMap().toString()).append(", ");
                }
                builder.append(segment.end().asMap().toString()).append("]");
                return Slices.utf8Slice(builder.toString());
            }
            else {
                return Slices.utf8Slice(builder.append(path.start().asMap().toString()).append("]").toString());
            }
        }
    }

    @Override
    public Object getObject(int field)
    {
        checkState(!closed, "cursor is closed");
        throw new PrestoException(Neo4jErrorCode.NEO4J_NOT_SUPPORT, String.format("neo4j connector doesn't support this kind of type: %s", columnHandles[field].getColumnType().getDisplayName()));
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        if (isPath) {
            return record.get(0).isNull();
        }
        else {
            String columnName = columnHandles[field].getColumnName();
            int num;
            if (columnName.substring(0, 4).equals("node")) {
                num = Integer.parseInt(columnName.substring(4, 5));
                if (columnName.matches("^node[0-9]\\d*$")) {
                    return nodes.get(num).size() == 0;
                }
                return nodes.get(num).get(columnName.substring(5)).isNull();
            }
            else {
                num = Integer.parseInt(columnName.substring(12, 13));
                if (columnName.matches("^relationship[0-9]\\d*$")) {
                    return relationships.get(num).size() == 0;
                }
                return relationships.get(num).get(columnName.substring(13)).isNull();
            }
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }

        session.close();
        closed = true;
    }
}
