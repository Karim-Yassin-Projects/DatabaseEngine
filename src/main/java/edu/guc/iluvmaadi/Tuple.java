package edu.guc.iluvmaadi;

import java.io.Serial;
import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable, Comparable<Tuple> {
    @Serial
    private static final long serialVersionUID = 1L;
    private final Vector<Comparable<Object>> values;

    public Tuple(Vector<Comparable<Object>> values) {
        this.values = values;
    }

    public Vector<Comparable<Object>> getValues() {
        return values;
    }

    public Comparable<Object> getKey() {
        return values.getFirst();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (i > 0)
                sb.append(",");
            sb.append(value);
        }
        return sb.toString();
    }

    @Override
    public int compareTo(Tuple o) {
        return getKey().compareTo(o.getKey());
    }
}
