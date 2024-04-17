package edu.guc.iluvmaadi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Index implements Serializable {

    private static final long serialVersionUID = 1L;
    private BPlusTree tree = new BPlusTree(DBApp.maximumRowsCountinPage + 1);

    public void clear() {
        tree = new BPlusTree(DBApp.maximumRowsCountinPage + 1);
    }

    static class IndexEntry implements Serializable, Comparable<IndexEntry> {
        private static final long serialVersionUID = 1L;

        private final Comparable indexKeyValue;
        private final Comparable clusteringKeyValue;
        private final int pageNumber;

        public IndexEntry(Comparable indexKeyValue, Comparable clusteringKeyValue, int pageNumber) {
            this.indexKeyValue = indexKeyValue;
            this.clusteringKeyValue = clusteringKeyValue;
            this.pageNumber = pageNumber;
        }

        public Comparable getIndexKeyValue() {
            return indexKeyValue;
        }

        public Comparable getClusteringKeyValue() {
            return clusteringKeyValue;
        }

        public int getPageNumber() {
            return pageNumber;
        }

        @Override
        public int compareTo(IndexEntry o) {
            int res = indexKeyValue.compareTo(o.indexKeyValue);
            if (res == 0) {
                if (o.clusteringKeyValue == null || clusteringKeyValue == null) {
                    return 0;
                }
                res = clusteringKeyValue.compareTo(o.clusteringKeyValue);
            }
            return res;
        }
    }

    Vector<IndexEntry> search(Comparable indexValueStart, Comparable indexValueEnd) {
        IndexEntry startEntry = new IndexEntry(indexValueStart, null, 0);
        IndexEntry endEntry = new IndexEntry(indexValueEnd, null, 0);
        Vector<Object> result = tree.search(startEntry, endEntry);
        Vector<IndexEntry> values = new Vector<>();
        for (Object o : result) {
            values.add((IndexEntry) o);
        }
        return values;
    }

    public void insert(Comparable indexKeyValue, Comparable clusteringKeyValue, int pageNumber) {
        IndexEntry pair = new IndexEntry(indexKeyValue, clusteringKeyValue, pageNumber);
        tree.insert(pair, pair);
    }

    public void delete(Comparable indexKeyValue, Comparable clusteringKeyValue) {
        tree.delete(new IndexEntry(indexKeyValue, clusteringKeyValue, 0));
    }

}
