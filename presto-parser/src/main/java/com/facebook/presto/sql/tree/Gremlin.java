package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Gremlin
        extends Relation
{
    private final String sentence;
    private final String connector;
    private final QualifiedName name;

    //Hong: Hardcore here. Need to be removed in the future.
    public Gremlin(NodeLocation location, String sentence)
    {
        this(Optional.of(location), sentence, "hugegraph", "gremlin");
    }

    public Gremlin(NodeLocation location, String sentence, String name)
    {
        this(Optional.of(location), sentence, "hugegraph", name);
    }

    public Gremlin(String sentence, String connector, String name)
    {
        this(Optional.empty(), sentence, connector, name);
    }

    public Gremlin(NodeLocation location, String sentence, String connector, String name)
    {
        this(Optional.of(location), sentence, connector, name);
    }

    private Gremlin(Optional<NodeLocation> location, String sentence, String connector, String name)
    {
        super(location);
        requireNonNull(sentence, "sentence is null");
        this.sentence = sentence;
        this.connector = connector;
        this.name = QualifiedName.of(name);
    }

    public String getSentence()
    {
        return sentence;
    }

    public String getConnector()
    {
        return connector;
    }

    public QualifiedName getName()
    {
        return name;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGremlin(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sentence);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Gremlin o = (Gremlin) obj;
        return Objects.equals(sentence, o.sentence) && Objects.equals(connector, o.connector) && Objects.equals(name, o.name);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("Sentence", sentence)
                .add("Connector", connector)
                .addValue(name)
                .toString();
    }

}
