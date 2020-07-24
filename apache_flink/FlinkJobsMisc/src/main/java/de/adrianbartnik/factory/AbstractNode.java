package de.adrianbartnik.factory;

public abstract class AbstractNode {

    private static final int DEFAULT_PARALLELISM = 4;

    protected final int parallelism;

    public AbstractNode() {
        this(DEFAULT_PARALLELISM);
    }

    public AbstractNode(int parallelism) {
        this.parallelism = parallelism;
    }
}
