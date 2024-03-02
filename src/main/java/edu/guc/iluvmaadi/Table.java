package edu.guc.iluvmaadi;

import java.util.Vector;

public class Table {

    private String tableName;
    private String clusteringKey;
    private Vector<Column> columns = new Vector<Column>();

    public Table(String tableName, Vector<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public Vector<Column> getColNameType() {
        return columns;
    }

    public void setClusteringKey(String clusteringKey) throws DBAppException{
        if(this.getClusteringKey() != null){
            throw new DBAppException("Clustering key already exists");
        }
        this.clusteringKey = clusteringKey;
    }

    public void addColumn(String colName, String colType) throws DBAppException{
        for(Column col : columns){
            if(col.getName().equals(colName)){
                throw new DBAppException("Column already exists");
            }
        }
        columns.add(new Column(colName, colType));
    }
}
