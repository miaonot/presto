package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Match extends Relation
{
    private final String node1;
    private final String node2;
    private final String pathcontrol;
    private final String graphname;

    public Match(String node1, String node2, String pathcontrol,String graphname){
        this(Optional.empty(), node1, node2, pathcontrol, graphname);
    }

    public Match(NodeLocation location, String node1, String node2, String pathcontrol)
    {
        this(Optional.of(location), node1, node2, pathcontrol, "graphtable");
    }

    public Match(NodeLocation location, String node1, String node2, String pathcontrol, String graphname)
    {
        this(Optional.of(location), node1, node2, pathcontrol, graphname);
    }

    private Match(Optional<NodeLocation> location, String node1, String node2, String pathcontrol, String graphname)
    {
        super(location);
        requireNonNull(node1, "node1 is null");
        requireNonNull(node1, "node2 is null");
        requireNonNull(pathcontrol, "pathcontrol is null");

        this.node1 = node1;
        this.node2 = node2;
        this.pathcontrol = pathcontrol;
        this.graphname = graphname;
    }

    public String getNode1() {
        return node1;
    }

    public String getNode2() {
        return node2;
    }

    public String getPathcontrol() {
        return pathcontrol;
    }

    public String getGraphname() {
        return graphname;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMatch(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Match o = (Match) obj;
        return Objects.equals(node1, o.node1) && Objects.equals(node2, o.node2) && Objects.equals(pathcontrol, o.pathcontrol);

    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("node1", node1)
                .add("node2", node2)
                .add("pathcontrol",pathcontrol)
                .add("graphname",graphname)
                .toString();
    }
}
