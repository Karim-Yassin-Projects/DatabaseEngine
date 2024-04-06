package edu.guc.iluvmaadi;

public class SQLTerm {

    private final String tableName;
    private final String columnName;
    private final String operator;
    private final Comparable value;

    public SQLTerm(String tableName, String columnName, String operator, Comparable value) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getOperator() {
        return operator;
    }

    public Comparable getValue() {
        return value;
    }
}