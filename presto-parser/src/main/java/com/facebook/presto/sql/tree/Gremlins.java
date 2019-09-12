package com.facebook.presto.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Gremlins
        extends Statement
{
    private final Optional<List<String>> Sentence;
    private final boolean withGremlinsOption;

    public Gremlins(Optional<List<String>> Sentence, boolean withGremlinsOption)
    {
        this(Optional.empty(), Sentence, withGremlinsOption);
    }

    public Gremlins(NodeLocation location,Optional<List<String>> Sentence, boolean withGremlinsOption)
    {
        this(Optional.of(location), Sentence, withGremlinsOption);
    }

    private Gremlins(Optional<NodeLocation> location, Optional<List<String>> Sentence, boolean withGremlinsOption)
    {
        super(location);
        requireNonNull(Sentence, "sentence is null");
        this.Sentence = Sentence.map(ImmutableList::copyOf);
        this.withGremlinsOption = withGremlinsOption;
    }

    public Optional<List<String>> getSentence()
    {
        return Sentence;
    }

    public boolean isWithGremlinsOption()
    {
        return withGremlinsOption;
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGremlins(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Sentence, withGremlinsOption);
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
        Gremlins o = (Gremlins) obj;
        return Objects.equals(Sentence, o.Sentence)&&
                Objects.equals(withGremlinsOption, o.withGremlinsOption);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("Sentence", Sentence)
                .toString();
    }

}
