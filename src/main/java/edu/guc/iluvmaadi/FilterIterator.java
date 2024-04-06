package edu.guc.iluvmaadi;

import java.util.Iterator;

public class FilterIterator implements Iterator<Tuple> {

    private final Iterator<Tuple> innerIterator;
    private final Query query;
    private Tuple nextTuple;

    public FilterIterator(Iterator<Tuple> innerIterator, Query query) {
        this.innerIterator = innerIterator;
        this.query = query;
        nextTuple = findNext();
    }

    private Tuple findNext() {
        while(innerIterator.hasNext()) {
            Tuple tuple = innerIterator.next();
            try {
                if(query.match(tuple)) {
                    return tuple;
                }
            } catch (DBAppException e) {
                return null;
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        return nextTuple != null;
    }

    @Override
    public Tuple next() {
        Tuple result = nextTuple;
        nextTuple = findNext();
        return result;
    }
}
