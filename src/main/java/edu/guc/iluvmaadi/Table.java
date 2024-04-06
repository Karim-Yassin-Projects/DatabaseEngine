package edu.guc.iluvmaadi;

import java.io.*;
import java.util.Iterator;
import java.util.Vector;

public class Table implements Iterable<Tuple> {


    private final Vector<Column> columns;
    private final String tableName;
    private final TableInfo tableInfo;


    public Table(String tableName) throws DBAppException {
        this.tableName = tableName;
        columns = new Vector<>();
        tableInfo = TableInfo.loadTableInfo(tableName);
    }

    public String getTableName() {
        return tableName;
    }

    public void addColumn(String colName, String colType) throws DBAppException {
        for (Column column : columns) {
            if (column.getName().equals(colName)) {
                throw new DBAppException("Column " + colName + " already exists");
            }
        }

        Column column = new Column(colName, colType);
        columns.add(column);
    }

    public String getClusteringKey() {
        return columns.getFirst().getName();
    }

    public void setClusteringKey(String clusteringKey) throws DBAppException {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(clusteringKey)) {
                if (i == 0) {
                    return;
                }
                Column temp = columns.getFirst();
                columns.set(0, columns.get(i));
                columns.set(i, temp);
                return;
            }
        }
        throw new DBAppException("Column " + clusteringKey + " does not exist");
    }

    public Vector<Column> getColumns() {
        return columns;
    }

    public int getColumnIndex(String colName) throws DBAppException {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(colName)) {
                return i;
            }
        }
        throw new DBAppException("Column " + colName + " does not exist");
    }

    public Column getColumn(String colName) throws DBAppException {
        for (Column column : columns) {
            if (column.getName().equals(colName)) {
                return column;
            }
        }
        throw new DBAppException("Column " + colName + " does not exist");
    }

    public void insert(Tuple tuple) throws DBAppException {
        // Check the number of values to match the number of columns
        if (tuple.getValues().size() != columns.size()) {
            throw new DBAppException("Invalid number of values");
        }
        // Check the type of each value to match the type of the corresponding column
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (!column.getType().equals(tuple.getValues().get(i).getClass().getName())) {
                throw new DBAppException("Invalid value type for column " + column.getName());
            }
        }

        // Find the page index to insert the tuple
        int pageIndex = findPageForInsert(tuple.getKey());
        PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);
        Page page = loadPage(pageInfo.getPageNumber());
        Tuple existing = page.findKey(tuple.getKey());
        if (existing != null) {
            throw new DBAppException("Duplicate key: " + tuple.getKey());
        }

        insertIntoPage(page, pageIndex, tuple);
        tableInfo.saveTableInfo();
    }

    public void deleteFromPage(Tuple tuple) throws DBAppException {
        int pageIndex = findPageForDelete(tuple.getKey());
        PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);
        Page page = loadPage(pageInfo.getPageNumber());
        if (page.delete(tuple)) {
            savePage(pageInfo.getPageNumber(), page);

            if (page.getTuples().isEmpty()) {
                tableInfo.getPagesInfo().remove(pageIndex);
            } else {
                pageInfo.setMax(page.getTuples().lastElement().getKey());
                pageInfo.setMin(page.getTuples().firstElement().getKey());
                pageInfo.setSize(page.getTuples().size());
                tableInfo.saveTableInfo();
            }
        }
    }
    private int findPageForUpdate(Comparable<Object> key) {
        for (int i = 0; i < tableInfo.getPagesInfo().size(); i++) {
            PageInfo pageInfo = tableInfo.getPagesInfo().get(i);
            if (key.compareTo(pageInfo.getMin()) > 0 && key.compareTo(pageInfo.getMax()) < 0) {
                return i;
            }
        }
        return -1;
    }
    public void update(Comparable<Object> key, Vector<Comparable<Object>> values) throws DBAppException {
        int pageIndex = findPageForUpdate(key);
        PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);
        Page page = loadPage(pageInfo.getPageNumber());
        Tuple existing = page.findKey(key);
        if (existing == null) {
            return;
        }
        if (values.size() != columns.size()) {
            throw new DBAppException("Invalid number of values");
        }
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            if (!column.getType().equals(values.get(i).getClass().getName())) {
                throw new DBAppException("Invalid value type for column " + column.getName());
            }
        }
        if (page.update(key, values)) {
            savePage(pageInfo.getPageNumber(), page);
        }
    }

    private void insertIntoPage(Page page, int pageIndex, Tuple tuple) throws DBAppException {
        PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);
        if (page.isFull()) {
            Tuple lastTuple = page.getTuples().lastElement();
            PageInfo nextPageInfo;
            if (pageIndex == tableInfo.getPagesInfo().size() - 1) {
                // This is the last page, so create a new page
                nextPageInfo = new PageInfo(tableInfo.getNextPageNumber(), lastTuple.getKey(), lastTuple.getKey());
                tableInfo.getPagesInfo().add(nextPageInfo);
            } else {
                // next page info is the next page in the list
                nextPageInfo = tableInfo.getPagesInfo().get(pageIndex + 1);
            }

            Page nextPage = loadPage(nextPageInfo.getPageNumber());
            insertIntoPage(nextPage, pageIndex, lastTuple);
            page.getTuples().removeLast();
            pageInfo.setMax(page.getTuples().lastElement().getKey());
        }
        page.insert(tuple);
        savePage(pageInfo.getPageNumber(), page);
        if (pageInfo.getMax().compareTo(tuple.getKey()) < 0) {
            pageInfo.setMax(tuple.getKey());
        }
        if (pageInfo.getMin().compareTo(tuple.getKey()) > 0) {
            pageInfo.setMin(tuple.getKey());
        }
        pageInfo.setSize(page.getTuples().size());
    }

    private Page loadPage(int pageNumber) throws DBAppException {
        File file = new File(getPagePath(pageNumber));
        if (!file.exists()) {
            return new Page();
        }

        try (FileInputStream fileInputStream = new FileInputStream(file.getAbsoluteFile())) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
                return (Page) objectInputStream.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException("Error loading page " + pageNumber + " for table " + tableName, e);
        }
    }

    private void savePage(int pageNumber, Page page) throws DBAppException {
        File file = new File(getPagePath(pageNumber));
        // If page is empty delete the file
        if (page.getTuples().isEmpty()) {
            if (file.exists()) {
                if (!file.delete()) {
                    throw new DBAppException("Error deleting file " + file.getAbsolutePath());
                }
            }
            return;
        }
        File parent = file.getParentFile();
        if (!parent.exists()) {
            if (!parent.mkdirs()) {
                throw new DBAppException("Error creating directory " + parent.getAbsolutePath());
            }

        }
        try (FileOutputStream fileOutputStream = new FileOutputStream(file.getAbsoluteFile())) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
                objectOutputStream.writeObject(page);
            }
        } catch (IOException e) {
            throw new DBAppException("Error loading page " + pageNumber + " for table " + tableName, e);
        }
    }

    private int compareKeyToPageInfo(int pageIndex, Comparable<Object> key) {
        PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);

        if (key.compareTo(pageInfo.getMin()) < 0) {
            if (pageIndex == 0) {
                return 0;
            }

            PageInfo prevPageInfo = tableInfo.getPagesInfo().get(pageIndex - 1);
            if (prevPageInfo.isFull() && key.compareTo(prevPageInfo.getMax()) > 0) {
                return 0;
            }
            return -1;
        }
        if (key.compareTo(pageInfo.getMax()) > 0) {
            if (pageIndex == tableInfo.getPagesInfo().size() - 1) {
                if (pageInfo.isFull()) {
                    return 1;
                }
                return 0;
            }
            PageInfo nextPageInfo = tableInfo.getPagesInfo().get(pageIndex + 1);
            if (!pageInfo.isFull() && key.compareTo(nextPageInfo.getMin()) < 0) {
                return 0;
            }
            return 1;
        }
        return 0;
    }

    private int findPageForDelete(Comparable<Object> key) {
        for (int i = 0; i < tableInfo.getPagesInfo().size(); i++) {
            PageInfo pageInfo = tableInfo.getPagesInfo().get(i);
            if (key.compareTo(pageInfo.getMin()) >= 0 && key.compareTo(pageInfo.getMax()) <= 0) {
                return i;
            }
        }
        return -1;
    }

    private int findPageForInsert(Comparable<Object> key) {
        int low = 0;
        int high = tableInfo.getPagesInfo().size() - 1;
        while (low <= high) {
            int mid = (low + high) / 2;
            int cmp = compareKeyToPageInfo(mid, key);
            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                return mid;
            }
        }
        PageInfo pageInfo = new PageInfo(tableInfo.getNextPageNumber(), key, key);
        tableInfo.getPagesInfo().add(pageInfo);
        return tableInfo.getPagesInfo().size() - 1;
    }

    private String getPagePath(int pageNumber) {
        return "DBEngine-Data" + "/" + tableName + "_" + pageNumber + ".class";
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new TableIterator();
    }

    private class TableIterator implements Iterator<Tuple> {
        private int pageIndex;
        private int tupleIndex;
        private Page page;
        private int loadedPageNumber;

        public TableIterator() {
            pageIndex = 0;
            tupleIndex = 0;

        }

        public boolean hasNext() {
            if (pageIndex >= tableInfo.getPagesInfo().size()) {
                return false;
            }
            PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);
            return tupleIndex < pageInfo.getSize();
        }

        public Tuple next() {
            if (pageIndex >= tableInfo.getPagesInfo().size()) {
                return null;
            }
            PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);
            if (page == null || pageInfo.getPageNumber() != loadedPageNumber) {
                try {
                    page = loadPage(pageInfo.getPageNumber());
                    loadedPageNumber = pageInfo.getPageNumber();
                } catch (DBAppException e) {
                    throw new RuntimeException(e);
                }
            }
            Tuple tuple = page.getTuples().get(tupleIndex);
            tupleIndex++;
            if (tupleIndex >= pageInfo.getSize()) {
                pageIndex++;
                tupleIndex = 0;
            }
            return tuple;
        }
    }
}
