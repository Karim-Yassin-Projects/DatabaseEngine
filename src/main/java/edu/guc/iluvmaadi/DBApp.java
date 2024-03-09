package edu.guc.iluvmaadi;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;


public class DBApp {

    static Hashtable<String, Table> tables = new Hashtable<String, Table>();
    public static int pageSize;


    public DBApp( ){

    }

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init( ) throws DBAppException {
        readConfigFile();
    }



    private String getFilePath(){
        return "src/main/resources/DBApp.config";
    }

    private void readConfigFile() throws DBAppException {
        Properties properties = new Properties();
        try {
            try (FileInputStream fileInputStream = (FileInputStream) getClass().getModule().getResourceAsStream("DBApp.config")) {
                properties.load(fileInputStream);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String strPageSize = properties.getProperty("MaximumRowsCountinPage");
            if (strPageSize != null) {
                pageSize = Integer.parseInt(strPageSize);
                if (pageSize < 1) {
                    throw new DBAppException("Page size must be greater than 0");
                }
            } else {
                throw new DBAppException("Page size not found in config file");
            }
        } catch (NumberFormatException e) {
            //ignore
        } catch (DBAppException e) {
            //ignore
        }
    }

    public Table getTable(String tableName) throws DBAppException{
        for(Table table: tables.values()){
            if(table.getTableName().equals(tableName)){
                return table;
            }
        }
        throw new DBAppException("Table not found");
    }


    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    public static void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType) throws DBAppException{
        for(Table table: tables.values()){
            if(table.getTableName().equals(strTableName)) {
                throw new DBAppException("Table already exists");
            }
        }
        Table newTable = new Table(strTableName);
        for(String colName: htblColNameType.keySet()){
            newTable.addColumn(colName, htblColNameType.get(colName));
        }
        tables.put(strTableName, newTable);
        newTable.setClusteringKey(strClusteringKeyColumn);
        saveMetaData();
    }



    public static void saveMetaData(){
        String csvFileName = "metadata.csv";
        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(csvFileName, false))){
            for(Table table: tables.values()){
                for(Column col: table.getColNameType()){
                    bufferedWriter.write(table.getTableName() + ",");
                    bufferedWriter.write(col.getName() + ",");
                    bufferedWriter.write(col.getType() + ",");
                    if(col.getName().equals(table.getClusteringKey())){
                        bufferedWriter.write("True,");
                    }
                    else{
                        bufferedWriter.write("False,");
                    }

                    if(col.getIndexName() != null){
                        bufferedWriter.write(col.getIndexName() + ",");
                        bufferedWriter.write(  "B+Tree");
                        bufferedWriter.write("\n");
                    }
                    else{
                        bufferedWriter.write("null,null");
                        bufferedWriter.write("\n");
                    }
                }

            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }


    // following method creates a B+tree index
    public void createIndex(String   strTableName,
                            String   strColName,
                            String   strIndexName) throws DBAppException{

        if(getTable(strTableName) == null){
            throw new DBAppException("Table not found");
        }
        if(getTable(strTableName).getColumn(strColName) == null){
            throw new DBAppException("Column not found");
        }
        if(getTable(strTableName).getColumn(strColName).getIndexName() != null){
            throw new DBAppException("Index already exists");
        }
        getTable(strTableName).getColumn(strColName).setIndexName(strIndexName);
        saveMetaData();
        //lessa el bplustree msh mawgoud
    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object>  htblColNameValue) throws DBAppException{

        throw new DBAppException("not implemented yet");
    }


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue   )  throws DBAppException{

        throw new DBAppException("not implemented yet");
    }


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException{

        throw new DBAppException("not implemented yet");
    }


    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[]  strarrOperators) throws DBAppException{

        return null;
    }


   public static void main( String[] args ){

   }


}
