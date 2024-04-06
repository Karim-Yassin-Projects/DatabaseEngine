package edu.guc.iluvmaadi;

import java.util.ArrayList;
import java.util.Collections;

public class Query {
    private final Table table;
    private final SQLTerm[] sqlTerms;
    private final String[] operators;

    private final static String[] logicalOperators = {"AND", "OR", "XOR"};

    public Query(Table table, SQLTerm[] sqlTerms, String[] operators) {

        if ((sqlTerms.length == 0 && operators.length != 0)
                || (sqlTerms.length > 0 && sqlTerms.length - 1 != operators.length)) {
            throw new IllegalArgumentException("Invalid number of operators");
        }
        this.table = table;
        this.sqlTerms = sqlTerms;
        this.operators = operators;
    }

    public boolean match(Tuple tuple) throws DBAppException {
        ArrayList<Boolean> results = new ArrayList<>();
        for (SQLTerm sqlTerm : sqlTerms) {
            String colName = sqlTerm.getColumnName();
            int colIndex = table.getColumnIndex(colName);

            Comparable<Object> value = tuple.getValues().get(colIndex);
            String operator = sqlTerm.getOperator();
            Comparable<Object> objValue = sqlTerm.getValue();
            results.add(compare(value, operator, objValue));
        }
        ArrayList<String> operatorsList = new ArrayList<>();
        Collections.addAll(operatorsList, operators);

        for (String logicalOperator : logicalOperators) {
            while (operatorsList.contains(logicalOperator)) {
                int index = operatorsList.indexOf(logicalOperator);
                boolean result1 = results.get(index);
                boolean result2 = results.get(index + 1);
                boolean result;
                switch (logicalOperator) {
                    case "AND":
                        result = result1 && result2;
                        break;
                    case "OR":
                        result = result1 || result2;
                        break;
                    case "XOR":
                        result = result1 ^ result2;
                        break;
                    default:
                        throw new DBAppException("Invalid logical operator: " + logicalOperator);
                }
                results.remove(index);
                results.remove(index);
                results.add(index, result);
                operatorsList.remove(index);
            }
        }
        return results.get(0);
    }

    private boolean compare(Comparable<Object> value, String operator, Comparable<Object> objValue) throws DBAppException {
        int compareResult = value.compareTo(objValue);
        switch (operator) {
            case "=":
                return compareResult == 0;
            case "!=":
                return compareResult != 0;
            case ">":
                return compareResult > 0;
            case ">=":
                return compareResult >= 0;
            case "<":
                return compareResult < 0;
            case "<=":
                return compareResult <= 0;
            default:
                throw new DBAppException("Invalid operator: " + operator);
        }
    }
}
