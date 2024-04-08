package edu.guc.iluvmaadi;

import java.io.Serializable;
import java.util.ArrayList;

public class Index implements Serializable {

    private BPlusTree tree = new BPlusTree(3);

    public void clear() {
        tree = new BPlusTree(3);
    }

    static class KeyValuePair implements Serializable, Comparable<KeyValuePair> {
        private final Comparable key;
        private final Comparable value;

        public KeyValuePair(Comparable key, Comparable value) {
            this.key = key;
            this.value = value;
        }

        public Comparable getKey() {
            return key;
        }

        public Comparable getValue() {
            return value;
        }

        @Override
        public int compareTo(KeyValuePair o) {
            int res = key.compareTo(o.key);
            if (res == 0) {
                if (o.value == null || value == null) {
                    return 0;
                }
            }
            return res;
        }
    }

    public ArrayList<Object> search(Comparable start, Comparable end) {
        ArrayList<Object> result = tree.search(new KeyValuePair(start, null), new KeyValuePair(end, null));
        ArrayList<Object> values = new ArrayList<>();
        for (Object o : result) {
            values.add(((KeyValuePair) o).getValue());
        }
        return values;
    }

    public void insert(Comparable key, Comparable value) {
        KeyValuePair pair = new KeyValuePair(key, value);
        tree.insert(pair, pair);
    }

    public void delete(Comparable key, Comparable value) {
        tree.delete(new KeyValuePair(key, value));
    }

    public void delete(Comparable key) {
        tree.delete(new KeyValuePair(key, null));
    }

}
