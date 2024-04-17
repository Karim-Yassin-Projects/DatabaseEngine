package edu.guc.iluvmaadi;
/*
 * @author Wael Abouelsaadat
 */

import java.io.*;
import java.util.*;

public class DBApp {

    public static int maximumRowsCountinPage = 200;
    public static String databasePath = "dbengine-data";
    public static String fileExtension = ".class";
    
    public final Hashtable<String, Table> tables = new Hashtable<>();


    public DBApp() throws DBAppException {
        init();
    }

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init() throws DBAppException {
        loadConfiguration();
        loadMetadata();
    }

    private void loadConfiguration() {
        Properties properties = new Properties();
        try {
            try (InputStream in = getClass().getModule().getResourceAsStream("DBApp.config")) {
                properties.load(in);
            }
        } catch (IOException e) {
            //e.printStackTrace();
        }
        String strMaximumRowsCountinPage = properties.getProperty("MaximumRowsCountinPage");
        if (strMaximumRowsCountinPage != null) {
            maximumRowsCountinPage = Integer.parseInt(strMaximumRowsCountinPage);
        }
        
        String strDbPath = properties.getProperty("DatabasePath");
        if (strDbPath != null) {
            databasePath = strDbPath;
        }
        
        String strFileExtension = properties.getProperty("FileExtension");
        if (strFileExtension != null) {
            fileExtension = strFileExtension;
        }
    }


    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType) throws DBAppException {

        if (tables.containsKey(strTableName)) {
            throw new DBAppException("Table " + strTableName + " already exists");
        }
        Table table = new Table(strTableName);
        for (String colName : htblColNameType.keySet()) {
            table.addColumn(colName, htblColNameType.get(colName));
        }
        table.setClusteringKey(strClusteringKeyColumn);

        tables.put(strTableName, table);
        saveMetadata();
    }

    public void dropTable(String strTableName) throws DBAppException {
        if (!tables.containsKey(strTableName)) {
            throw new DBAppException("Table " + strTableName + " does not exist");
        }
        tables.remove(strTableName);
        saveMetadata();
    }

    public Table getTable(String strTableName) {
        if (!tables.containsKey(strTableName)) {
            return null;
        }
        return tables.get(strTableName);
    }

    private String getMetadataPath() {
        return databasePath  + "/metadata.csv";
    }

    private void loadMetadata() throws DBAppException {
        File file = new File(getMetadataPath());
        if (!file.exists()) {
            return;
        }
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(getMetadataPath()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] parts = line.split(",");
                String tableName = parts[0];
                String colName = parts[1];
                String colType = parts[2];
                String indexName = parts[4];
                String indexType = parts[5];

                boolean isClusteringKey = Boolean.parseBoolean(parts[3]);
                if (!tables.containsKey(tableName)) {
                    tables.put(tableName, new Table(tableName));
                }
                Table table = tables.get(tableName);
                table.addColumn(colName, colType);
                if (isClusteringKey) {
                    table.setClusteringKey(colName);
                }

                if (!indexName.equals("null")) {
                    if (!indexType.equals("B+tree")) {
                        throw new DBAppException("Invalid index type: " + indexType);
                    }
                    Column column = table.getColumn(colName);
                    column.setIndexName(indexName);
                }
            }

            for (Table table : tables.values()) {
                for (Column column : table.getColumns()) {
                    if (column.getIndexName() != null) {
                        table.createIndex(column.getName());
                    }
                }
            }
        } catch (IOException e) {
            throw new DBAppException("Error loading metadata", e);
        }
    }

    private void saveMetadata() throws DBAppException {
        // Create the directory if it doesn't exist
        File file = new File(getMetadataPath());
        File directory = file.getParentFile();
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new DBAppException("Error creating directory " + directory);
            }
        }

        try (FileWriter fileWriter = new FileWriter(getMetadataPath(), false)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                for (Table table : tables.values()) {
                    for (Column column : table.getColumns()) {
                        bufferedWriter.write(table.getTableName());
                        bufferedWriter.write(",");
                        bufferedWriter.write(column.getName());
                        bufferedWriter.write(",");
                        bufferedWriter.write(column.getType());
                        bufferedWriter.write(",");
                        if (column.getName().equals(table.getClusteringKey())) {
                            bufferedWriter.write("True");
                        } else {
                            bufferedWriter.write("False");
                        }
                        if (column.getIndexName() != null) {
                            bufferedWriter.write(",");
                            bufferedWriter.write(column.getIndexName());
                            bufferedWriter.write(",");
                            bufferedWriter.write("B+tree");
                        } else {
                            bufferedWriter.write(",null,null");
                        }
                        bufferedWriter.newLine();
                    }
                }
            }
        } catch (IOException e) {
            throw new DBAppException("Error saving metadata", e);
        }
    }


    // following method creates a B+tree index
    public void createIndex(String strTableName,
                            String strColName,
                            String strIndexName) throws DBAppException {

        Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("Table " + strTableName + " does not exist");
        }

        Column column = table.getColumn(strColName);
        if (column == null) {
            throw new DBAppException("Column " + strColName + " does not exist");
        }

        if (column.getIndexName() != null) {
            throw new DBAppException("Column " + strColName + " already has an index");
        }
        column.setIndexName(strIndexName);
        saveMetadata();
    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    @SuppressWarnings("unchecked")
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

        Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("Table " + strTableName + " does not exist");
        }
        Vector<Comparable<Object>> values = new Vector<>();
        for (Column column : table.getColumns()) {
            Object value = htblColNameValue.get(column.getName());
            if (value == null) {
                throw new DBAppException("Column " + column.getName() + " is missing");
            }
            if (!column.getType().equals(value.getClass().getName())) {
                throw new DBAppException("Invalid value type for column " + column.getName());
            }
            values.add((Comparable<Object>) value);
        }
        Tuple tuple = new Tuple(values);
        table.insert(tuple);
    }


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {

        Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("Table " + strTableName + " does not exist");
        }

        Comparable key;
        if (table.getColumns().get(0).getType().equals(Integer.class.getName())) {
            Integer intVal = Integer.parseInt(strClusteringKeyValue);
            key = intVal;
        } else if (table.getColumns().get(0).getType().equals(Double.class.getName())) {
            Double doubleVal = Double.parseDouble(strClusteringKeyValue);
            key = doubleVal;
        } else {
            key = strClusteringKeyValue;
        }
        Vector<Comparable<Object>> values = new Vector<>();
        for (Column column : table.getColumns()) {
            Object value = htblColNameValue.get(column.getName());
            if (value == null) {
                values.add(null);
                continue;
            }
            if (!column.getType().equals(value.getClass().getName())) {
                throw new DBAppException("Invalid value type for column " + column.getName());
            }
            //noinspection unchecked
            values.add((Comparable<Object>) value);
        }
        values.set(0, key); // set the clustering key value (first column)
        Tuple tuple = new Tuple(values);
        table.update(tuple.getKey(), values);


    }



    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue entries are ANDed together
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

        Table table = getTable(strTableName);
        if (table == null) {
            throw new DBAppException("Table " + strTableName + " does not exist");
        }

        for (Column column : table.getColumns()) {
            Object value = htblColNameValue.get(column.getName());
            if (value == null) {
                continue;
            }
            if (!column.getType().equals(value.getClass().getName())) {
                throw new DBAppException("Invalid value type for column " + column.getName());
            }
            //noinspection unchecked
        }
        table.delete(htblColNameValue);

    }

    public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms,
                                           String[] strarrOperators) throws DBAppException {
        Table table = getTable(arrSQLTerms[0].getTableName());
        if (table == null) {
            throw new DBAppException("Table " + arrSQLTerms[0].getTableName() + " does not exist");
        }
        Query query = new Query(table, arrSQLTerms, strarrOperators);


        Iterator<Tuple> iterator = table.iterator(query);
        return new FilterIterator(iterator, query);
    }


}