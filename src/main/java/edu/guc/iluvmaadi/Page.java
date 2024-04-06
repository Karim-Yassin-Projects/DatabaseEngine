package edu.guc.iluvmaadi;

import java.io.Serial;
import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final Vector<Tuple> tuples = new Vector<>();

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public boolean isFull() {
        return tuples.size() == DBApp.maximumRowsCountinPage;
    }

    public void insert(Tuple tuple) throws DBAppException {
        if (isFull()) {
            throw new DBAppException("Page is full");
        }
        int index = binarySearch(tuple.getKey());
        if (index >= 0) {
            throw new DBAppException("Duplicate key: " + tuple.getKey());
        }
        index = -(index + 1);
        tuples.add(index, tuple);
    }

    public boolean update(Comparable<Object> key, Vector<Comparable<Object>> values) {
        int index = binarySearch(key);
        if (index < 0) {
            return false;
        }
        tuples.set(index, new Tuple(values));
        return true;
    }

    public boolean delete(Tuple tuple) {
        int index = binarySearch(tuple.getKey());
        if (index < 0) {
            return false;
        }
        var toRemove = tuples.get(index);
        for (int i = 0; i < toRemove.getValues().size(); i++) {
            if (!toRemove.getValues().get(i).equals(tuple.getValues().get(i))) {
                return false;
            }
        }
        tuples.remove(index);
        return true;
    }

    public Tuple findKey(Comparable<Object> key) {
        int index = binarySearch(key);
        if (index < 0) {
            return null;
        }
        return tuples.get(index);
    }

    private int binarySearch(Comparable<Object> key) {
        int low = 0;
        int high = tuples.size() - 1;

        while (low <= high) {
            int mid = (low + high) / 2;
            Comparable<Object> midVal = tuples.get(mid).getKey();
            int cmp = midVal.compareTo(key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < tuples.size(); i++) {
            Tuple tuple = tuples.get(i);
            if (i > 0)
                sb.append(System.lineSeparator());
            sb.append(tuple);
        }
        sb.append("]");
        return sb.toString();
    }
}
