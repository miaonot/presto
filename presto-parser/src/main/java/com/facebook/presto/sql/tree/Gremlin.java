package com.facebook.presto.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Gremlin
        extends Relation
{
    private final String Sentence;

    public Gremlin(String Sentence)
    {
        this(Optional.empty(), Sentence);
    }

    public Gremlin(NodeLocation location, String Sentence)
    {
        this(Optional.of(location), Sentence);
    }

    private Gremlin(Optional<NodeLocation> location, String Sentence)
    {
        super(location);
        requireNonNull(Sentence, "sentence is null");
        this.Sentence = Sentence;
    }

    public String getSentence()
    {
        return Sentence;
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
        return Objects.hash(Sentence);
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
        return Objects.equals(Sentence, o.Sentence);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("Sentence", Sentence)
                .toString();
    }

}
