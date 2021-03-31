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
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class Neo4jRecordCursor
        implements RecordCursor
{
    public static final Logger log = Logger.get(Neo4jRecordCursor.class);

    private final Neo4jColumnHandle[] columnHandles;
    private final Neo4jClient neo4jClient;
    private final boolean isPath;
    private boolean isSequential;

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

        List<String> nodeTypes = split.getNodeTypes();
        List<String> relationshipTypes = split.getRelationshipTypes();
        List<String> nodeNames = split.getNodeNames();
        List<String> relationshipNames = split.getRelationshipNames();
        List<String> arguments = split.getArguments();
        Optional<List<String>> project = split.getProject();

        if (arguments.size() != 0) {
            isSequential = true;
            StringBuilder statement = new StringBuilder(
                    "match (start:node{id:1}), (end:node{id:13597}) \n" +
                            "call gds.alpha.shortestPath.astar.stream({\n" +
                            "  nodeProjection: {\n" +
                            "    node: {\n" +
                            "      properties: ['x', 'y']\n" +
                            "    }\n" +
                            "  },\n" +
                            "  relationshipProjection: {\n" +
                            "   \tresidential: {\n" +
                            "      \ttype: 'residential',\n" +
                            "      \torientation: 'undirected',\n" +
                            "      \tproperties: 'length'\n" +
                            "    }\n" +
                            "  },\n" +
                            "  startNode: start,\n" +
                            "  endNode: end,\n" +
                            "  relationshipWeightProperty: 'length',\n" +
                            "  propertyKeyLat: 'x',\n" +
                            "  propertyKeyLon: 'y'\n" +
                            "  })\n" +
                            "YIELD nodeId, cost\n" +
                            "RETURN gds.util.asNode(nodeId).id, cost;");
            session = neo4jClient.openSession();
            this.result = session.run(statement.toString());
            return;
        }

        StringBuilder statement = new StringBuilder("MATCH p=");
        int i;
        for (i = 0; i < relationshipTypes.size(); i++) {
            statement.append("(node").append(i).append(nodeTypes.get(i)).append(")-");
            statement.append("[relationship").append(i).append(relationshipTypes.get(i)).append("]-");
        }
        if (!nodeTypes.get(i).contains(":") && !nodeTypes.get(i).equals("")) {
            statement.append("(node").append(i).append(":").append((nodeTypes.get(i))).append(") ");
        }
        else {
            statement.append("(node").append(i).append(nodeTypes.get(i)).append(") ");
        }
        if (project.isPresent()) {
            isSequential = true;
            statement.append("RETURN ");
            List<String> assignments = project.get();
            i = 0;
            for (; i < project.get().size() - 1; i++) {
                statement.append(new StringBuilder(assignments.get(i)).insert(assignments.get(i).startsWith("node") ? 5 : 13, ".").toString()).append(", ");
            }
            statement.append(new StringBuilder(assignments.get(i)).insert(assignments.get(i).startsWith("node") ? 5 : 13, ".").toString()).append(" ");
        }
        else {
            statement.append("RETURN p ");
        }
        split.getLimitCount().ifPresent(count -> statement.append("LIMIT ").append(count));
//        String matchStatement = split.getSchemaName();
//        String token = null;
//        String statement;
//        if (matchStatement.contains("=")) {
//            String[] splitResult = matchStatement.split("=");
//            token = splitResult[0];
//        }
//        if (token != null) {
//            statement = "match " + matchStatement + " return" + token;
//        }
//        else {
//            statement = "match p = " + matchStatement + " return p";
//        }

        session = neo4jClient.openSession();
        this.result = session.run(statement.toString());
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

            if (record.get(0).type().name().equals("PATH")) {
                nodes = new ArrayList<>();
                record.get(0).asPath().nodes().forEach(nodes::add);

                relationships = new ArrayList<>();
                record.get(0).asPath().relationships().forEach(relationships::add);
            }

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
        if (isSequential) {
            return record.get(field).asBoolean();
        }
        String columnName = columnHandles[field].getColumnName();
        int num;
        if (columnName.startsWith("node")) {
            num = Integer.parseInt(columnName.substring(4, 5));
            return nodes.get(num).get(columnName.substring(5)).asBoolean();
        }
        if (columnName.startsWith("relationship")) {
            num = Integer.parseInt(columnName.substring(12, 13));
            return relationships.get(num).get(columnName.substring(13)).asBoolean();
        }
        return nodes.get(0).get(columnName).asBoolean();
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        if (isSequential) {
            return record.get(field).asLong();
        }
        String columnName = columnHandles[field].getColumnName();
        int num;
        if (columnName.startsWith("node")) {
            num = Integer.parseInt(columnName.substring(4, 5));
            return nodes.get(num).get(columnName.substring(5)).asLong();
        }
        if (columnName.startsWith("relationship")) {
            num = Integer.parseInt(columnName.substring(12, 13));
            return relationships.get(num).get(columnName.substring(13)).asLong();
        }
        return nodes.get(0).get(columnName).asLong();
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        if (isSequential) {
            return record.get(field).asDouble();
        }
        String columnName = columnHandles[field].getColumnName();
        int num;
        if (columnName.startsWith("node")) {
            num = Integer.parseInt(columnName.substring(4, 5));
            return nodes.get(num).get(columnName.substring(5)).asDouble();
        }
        if (columnName.startsWith("relationship")) {
            num = Integer.parseInt(columnName.substring(12, 13));
            return relationships.get(num).get(columnName.substring(13)).asDouble();
        }
        return nodes.get(0).get(columnName).asDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        String columnName = columnHandles[field].getColumnName();
        int type = columnHandles[field].getNeo4jTypeHandle().getType();
        if (!isPath && !isSequential) {
            int num;
            if (type == Types.ARRAY) {
                if (columnName.startsWith("node")) {
                    num = Integer.parseInt(columnName.substring(4, 5));
                    return Slices.utf8Slice(nodes.get(num).get(columnName.substring(5)).asList().toString());
                }
                if (columnName.startsWith("relationship")) {
                    num = Integer.parseInt(columnName.substring(12, 13));
                    return Slices.utf8Slice(relationships.get(num).get(columnName.substring(13)).asList().toString());
                }
                return Slices.utf8Slice(nodes.get(0).get(columnName).asList().toString());
            }
            else {
                if (columnName.startsWith("node")) {
                    num = Integer.parseInt(columnName.substring(4, 5));
                    if (columnName.matches("^node[0-9]\\d*$")) {
                        return Slices.utf8Slice(nodes.get(num).asMap().toString());
                    }
                    return Slices.utf8Slice(nodes.get(num).get(columnName.substring(5)).asString());
                }
                if (columnName.startsWith("relationship")) {
                    num = Integer.parseInt(columnName.substring(12, 13));
                    if (columnName.matches("^relationship[0-9]\\d*$")) {
                        return Slices.utf8Slice(relationships.get(num).asMap().toString());
                    }
                    return Slices.utf8Slice(relationships.get(num).get(columnName.substring(13)).asString());
                }
            }
            return Slices.utf8Slice(nodes.get(0).get(columnName).asString());
        }
        else if (isPath) {
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
        else {
            return Slices.utf8Slice(record.get(field).asString());
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
        if (isSequential) {
            return record.get(field).isNull();
        }
        else {
            String columnName = columnHandles[field].getColumnName();
            int num;
            if (columnName.startsWith("node")) {
                num = Integer.parseInt(columnName.substring(4, 5));
                if (columnName.matches("^node[0-9]\\d*$")) {
                    return nodes.get(num).size() == 0;
                }
                return nodes.get(num).get(columnName.substring(5)).isNull();
            }
            if (columnName.startsWith("relationship")) {
                num = Integer.parseInt(columnName.substring(12, 13));
                if (columnName.matches("^relationship[0-9]\\d*$")) {
                    return relationships.get(num).size() == 0;
                }
                return relationships.get(num).get(columnName.substring(13)).isNull();
            }
            return nodes.get(0).get(columnName).isNull();
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
