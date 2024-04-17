package edu.guc.iluvmaadi;

import java.io.*;
import java.util.*;

public class Table implements Iterable<Tuple> {


    private final Vector<Column> columns;
    private final String tableName;
    private final TableInfo tableInfo;

    private final Hashtable<String, Index> indices;


    public Table(String tableName) throws DBAppException {
        this.tableName = tableName;
        columns = new Vector<>();
        tableInfo = TableInfo.loadTableInfo(tableName);
        indices = new Hashtable<>();
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

    public void createIndex(String colName) throws DBAppException {
        if (indices.containsKey(colName)) {
            throw new DBAppException("Index on column " + colName + " already exists");
        }
        getColumnIndex(colName); // this will throw  if the column does not exist


        Index index = loadIndex(colName);
        indices.put(colName, index);

    }

    public String getIndexFileName(String colName) {
        return DBApp.databasePath + "/index_" + tableName + "_" + colName + DBApp.fileExtension;
    }

    private void populateIndex(Index index, String columnName) throws DBAppException {
        int columnIndex = getColumnIndex(columnName);
        for (int i = 0; i < tableInfo.getPagesInfo().size(); i++) {
            PageInfo pageInfo = tableInfo.getPagesInfo().get(i);
            int pageNumber = pageInfo.getPageNumber();
            Page page = loadPage(pageNumber);
            for (Tuple tuple : page.getTuples()) {
                Comparable columnValue = tuple.getValues().get(columnIndex);
                index.insert(columnValue, tuple.getKey(), pageNumber);
            }
        }
    }

    private Index loadIndex(String colName) throws DBAppException {
        File file = new File(getIndexFileName(colName));
        if (!file.exists()) {
            Index index = new Index();
            populateIndex(index, colName);
            saveIndex(colName, index);
            return index;
        }

        try (FileInputStream fileInputStream = new FileInputStream(file.getAbsoluteFile())) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
                return (Index) objectInputStream.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException("Error loading index for column " + colName + " in table " + tableName, e);
        }
    }

    private void saveIndex(String colName, Index index) throws DBAppException {
        File file = new File(getIndexFileName(colName));

        File parent = file.getParentFile();
        if (!parent.exists()) {
            if (!parent.mkdirs()) {
                throw new DBAppException("Error creating directory " + parent.getAbsolutePath());
            }

        }
        try (FileOutputStream fileOutputStream = new FileOutputStream(file.getAbsoluteFile())) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
                objectOutputStream.writeObject(index);
            }
        } catch (IOException e) {
            throw new DBAppException("Error loading index for column " + colName + " in table " + tableName, e);
        }
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
        int pageNumber = pageInfo.getPageNumber();
        Page page = loadPage(pageNumber);
        Tuple existing = page.findKey(tuple.getKey());
        if (existing != null) {
            throw new DBAppException("Duplicate key: " + tuple.getKey());
        }

        insertIntoPage(page, pageIndex, tuple);

        // Update all indices
        for (Map.Entry<String, Index> entry : indices.entrySet()) {
            int columnIndex = getColumnIndex(entry.getKey());
            Index index = entry.getValue();
            Comparable columnValue = tuple.getValues().get(columnIndex);
            index.insert(columnValue, tuple.getKey(), pageNumber);

            saveIndex(entry.getKey(), index);
        }
        tableInfo.saveTableInfo();
    }

    public boolean delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
        DeleteStrategy deleteStrategy;
        if (htblColNameValue.containsKey(getClusteringKey())) {
            deleteStrategy = new DeleteByClusteringKey();
        } else if (htblColNameValue.isEmpty()) {
            deleteStrategy = new DeleteAll();
        } else {
            deleteStrategy = new DeleteByLooping();
        }

        return deleteStrategy.delete(htblColNameValue);
    }

    private int findPage(Comparable<Object> key) {
        int hi, lo, mid;
        hi = tableInfo.getPagesInfo().size() - 1;
        lo = 0;
        while (lo <= hi) {
            mid = (hi + lo) / 2;
            PageInfo pageInfo = tableInfo.getPagesInfo().get(mid);
            if (key.compareTo(pageInfo.getMin()) >= 0 && key.compareTo(pageInfo.getMax()) <= 0) {
                return mid;
            }
            if (key.compareTo(pageInfo.getMin()) < 0) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return -1;
    }

    public void update(Comparable<Object> key, Vector<Comparable<Object>> values) throws DBAppException {
        int pageIndex = findPage(key);

        if (pageIndex < 0) {
            return;
        }
        PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);
        Page page = loadPage(pageInfo.getPageNumber());
        Tuple existing = page.findKey(key);
        if (existing == null) {
            return;
        }

        // delete
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(getClusteringKey(), key);
        if (!delete(htblColNameValue)) {
            return;
        }

        // insert
        Vector<Comparable<Object>> newValues = new Vector<>();
        for (int i = 0; i < columns.size(); i++) {
            if (values.get(i) == null) {
                newValues.add(existing.getValues().get(i));
            } else {
                newValues.add(values.get(i));
            }
        }

        Tuple tuple = new Tuple(newValues);
        insert(tuple);

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

    private void deletePage(int pageNumber) throws DBAppException {
        File file = new File(getPagePath(pageNumber));
        if (file.exists()) {
            if (!file.delete()) {
                throw new DBAppException("Error deleting file " + file.getAbsolutePath());
            }
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
        return DBApp.databasePath + "/" + tableName + "_" + pageNumber + DBApp.fileExtension;
    }

    private Comparable canUseClusteringKey(Query query) {
        if (query.hasOrOrXor()) {
            return null;
        }

        SQLTerm[] sqlTerms = query.getSqlTerms();
        for (SQLTerm sqlTerm : sqlTerms) {
            if (sqlTerm.getColumnName().equals(getClusteringKey()) && sqlTerm.getOperator().equals("=")) {
                return sqlTerm.getValue();
            }
        }
        return null;
    }

    private Comparable[] canUseIndex(String columnName, Query query) throws DBAppException {
        if (query.hasOrOrXor()) {
            return null;
        }

        Comparable min = query.getMinValue(columnName);
        Comparable max = query.getMaxValue(columnName);

        if (min == null && max == null) {
            return null;
        }

        if (min == null) {
            String colType = getColumn(columnName).getType();
            if (colType.equals("java.lang.Integer")) {
                min = Integer.MIN_VALUE;
            } else if (colType.equals("java.lang.Double")) {
                min = Double.MIN_VALUE;
            } else if (colType.equals("java.lang.String")) {
                min = "";
            }
        }

        if (max == null) {
            String colType = getColumn(columnName).getType();
            if (colType.equals("java.lang.Integer")) {
                max = Integer.MAX_VALUE;
            } else if (colType.equals("java.lang.Double")) {
                max = Double.MAX_VALUE;
            } else if (colType.equals("java.lang.String")) {
                max = "\uFFFF";
            }
        }

        return new Comparable[]{min, max};
    }

    public Iterator<Tuple> iterator(Query query) throws DBAppException {
        Comparable key = canUseClusteringKey(query);
        if (key != null) {
            return new ClusteringKeyIterator(key);
        }

        for (Map.Entry<String, Index> entry : indices.entrySet()) {
            String columnName = entry.getKey();
            Index index = entry.getValue();
            Comparable[] range = canUseIndex(columnName, query);

             if (range != null) {

                 // Handle condition where min value is greater than max value
                 if (range[0] != null && range[1] != null && range[0].compareTo(range[1]) > 0) {
                     return new EmptyIterator();
                 }

                 return new IndexIterator(index, range[0], range[1]);
            }
        }
        return new TableIterator();
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new TableIterator();
    }

    private class EmptyIterator implements Iterator<Tuple> {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Tuple next() {
            return null;
        }
    }

    private class ClusteringKeyIterator implements Iterator<Tuple> {
        private final Comparable key;
        private int pageIndex;
        private Tuple tuple;
        private Page page;

        public ClusteringKeyIterator(Comparable key) {
            this.key = key;
            pageIndex = findPage(key);
        }

        public boolean hasNext() {
            if (pageIndex < 0) {
                return false;
            }
            if (page == null) {
                try {
                    page = loadPage(tableInfo.getPagesInfo().get(pageIndex).getPageNumber());
                } catch (DBAppException e) {
                    throw new RuntimeException(e);
                }
            }
            if (tuple == null) {
                tuple = page.findKey(key);
            }
            return tuple != null;
        }

        public Tuple next() {
            if (pageIndex < 0) {
                return null;
            }
            Tuple result = tuple;
            pageIndex = -1;
            tuple = null;
            return result;
        }
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

    private class IndexIterator implements Iterator<Tuple> {

        private final Index index;
        private final Comparable start;
        private final Comparable end;
        private Vector<Index.IndexEntry> searchResult;
        private int indexInSearchResult;
        private Hashtable<Integer, Page> loadedPages = new Hashtable<>();

        public IndexIterator(Index index, Comparable start, Comparable end) {
            this.index = index;
            this.start = start;
            this.end = end;
        }


        @Override
        public boolean hasNext() {
            if (searchResult == null) {
                searchResult = index.search(start, end);
            }
            return indexInSearchResult < searchResult.size();
        }

        private Page loadAndCachePage(int pageNumber) throws DBAppException {
            if (loadedPages.containsKey(pageNumber)) {
                return loadedPages.get(pageNumber);
            }
            Page page = loadPage(pageNumber);
            loadedPages.put(pageNumber, page);
            return page;
        }

        @Override
        public Tuple next() {
            if (searchResult == null) {
                searchResult = index.search(start, end);
            }
            if (indexInSearchResult >= searchResult.size()) {
                return null;
            }
            Index.IndexEntry entry = searchResult.get(indexInSearchResult);
            indexInSearchResult++;
            try {
                Page page = loadAndCachePage(entry.getPageNumber());
                return page.findKey(entry.getClusteringKeyValue());
            } catch (DBAppException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void deleteFromIndices(Tuple tuple) throws DBAppException {

        for (Map.Entry<String, Index> entry : indices.entrySet()) {
            int columnIndex = getColumnIndex(entry.getKey());
            Index index = entry.getValue();
            Comparable columnValue = tuple.getValues().get(columnIndex);
            index.delete(columnValue, tuple.getKey());
        }
    }

    private void saveAllIndices() throws DBAppException {
        for (Map.Entry<String, Index> entry : indices.entrySet()) {
            saveIndex(entry.getKey(), entry.getValue());
        }
    }

    interface DeleteStrategy {
        boolean delete(Hashtable<String, Object> htblColNameValue) throws DBAppException;
    }

    // This strategy is used when the delete query is empty
    class DeleteAll implements DeleteStrategy {
        @Override
        public boolean delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {

            if (tableInfo.getPagesInfo().isEmpty()) {
                return false;
            }
            // Loop over all pages and delete all tuples
            for (int i = 0; i < tableInfo.getPagesInfo().size(); i++) {
                PageInfo pageInfo = tableInfo.getPagesInfo().get(i);
                // Delete the page without loading
                deletePage(pageInfo.getPageNumber());
            }

            // Clear the pages info
            tableInfo.getPagesInfo().clear();
            tableInfo.saveTableInfo();

            for (Map.Entry<String, Index> entry : indices.entrySet()) {
                Index index = entry.getValue();
                index.clear();
                saveIndex(entry.getKey(), index);
            }
            return true;
        }
    }

    // This strategy is used when the clustering key is not provided in the delete query
    class DeleteByLooping implements DeleteStrategy {
        @Override
        public boolean delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
            boolean deleted = false;
            // Create a query to match the values
            SQLTerm[] arrSQLTerms = new SQLTerm[htblColNameValue.size()];
            String[] strarrOperators = new String[htblColNameValue.size() - 1];
            int i = 0;
            for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
                arrSQLTerms[i++] = new SQLTerm(getTableName(), entry.getKey(), "=", (Comparable) entry.getValue());
            }
            for (int j = 0; j < htblColNameValue.size() - 1; j++) {
                strarrOperators[j] = "AND";
            }

            Query query = new Query(Table.this, arrSQLTerms, strarrOperators);

            // Loop over all pages and delete the tuples that match the query
            for (i = 0; i < tableInfo.getPagesInfo().size(); i++) {
                PageInfo pageInfo = tableInfo.getPagesInfo().get(i);
                Page page = loadPage(pageInfo.getPageNumber());
                boolean deletedFromPage = false;
                for (int j = 0; j < page.getTuples().size(); j++) {
                    Tuple tuple = page.getTuples().get(j);
                    if (query.match(tuple)) {
                        page.getTuples().remove(j);
                        deleteFromIndices(tuple);
                        deleted = true;
                        deletedFromPage = true;
                        j--;
                    }
                }
                if (deletedFromPage) {
                    // Save the page (savePage will delete the file if the page is empty)

                    savePage(pageInfo.getPageNumber(), page);
                    if (page.getTuples().isEmpty()) {
                        // If the page is empty, remove it from the table info
                        tableInfo.getPagesInfo().remove(i);
                        i--;
                    } else {
                        // Update the page info
                        pageInfo.setMax(page.getTuples().lastElement().getKey());
                        pageInfo.setMin(page.getTuples().firstElement().getKey());
                        pageInfo.setSize(page.getTuples().size());
                    }
                }
            }
            if (deleted) {
                tableInfo.saveTableInfo();
                saveAllIndices();
            }
            return deleted;
        }
    }


    // this strategy is used when the clustering key is provided in the delete query
    class DeleteByClusteringKey implements DeleteStrategy {
        @Override
        public boolean delete(Hashtable<String, Object> htblColNameValue) throws DBAppException {
            // get the clustering key value
            Comparable<Object> key = (Comparable<Object>) htblColNameValue.get(getClusteringKey());

            // find the page index that contains the key
            int pageIndex = findPage(key);

            // if the key is not found, return
            if (pageIndex < 0) {
                return false;
            }

            // load the page
            PageInfo pageInfo = tableInfo.getPagesInfo().get(pageIndex);
            Page page = loadPage(pageInfo.getPageNumber());

            // Delete from the page
            Tuple tuple = page.delete(key);
            if (tuple != null) {
                deleteFromIndices(tuple);
                // Save the page (savePage will delete the file if the page is empty)
                savePage(pageInfo.getPageNumber(), page);

                if (page.getTuples().isEmpty()) {
                    // If the page is empty, remove it from the table info
                    tableInfo.getPagesInfo().remove(pageIndex);
                } else {
                    // Update the page info
                    pageInfo.setMax(page.getTuples().lastElement().getKey());
                    pageInfo.setMin(page.getTuples().firstElement().getKey());
                    pageInfo.setSize(page.getTuples().size());
                    tableInfo.saveTableInfo();
                }
                saveAllIndices();
                return true;
            }
            return false;
        }

    }
}
