package edu.guc.iluvmaadi;

import java.util.Vector;

public class Table {

    private String tableName;
    private String clusteringKey;
    private Vector<Column> columns = new Vector<Column>();

    public Table(String tableName) {
        this.tableName = tableName;
        this.columns = new Vector<Column>();
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
        for(int i = 0; i < columns.size(); i++){
            if(columns.get(i).getName().equals(clusteringKey)){
                if(i == 0){
                    return;
                }
                Column temp = columns.getFirst();
                columns.set(0, columns.get(i));
                columns.set(i, temp);
                return;
            }

        }
    }

    public void addColumn(String colName, String colType) throws DBAppException{
        for(Column col : columns){
            if(col.getName().equals(colName)){
                throw new DBAppException("Column already exists");
            }
        }
        columns.add(new Column(colName, colType));
    }

    public Column getColumn(String colName) throws DBAppException{
        for(Column col : columns){
            if(col.getName().equals(colName)){
                return col;
            }
        }
        throw new DBAppException("Column does not exist");
    }
}
