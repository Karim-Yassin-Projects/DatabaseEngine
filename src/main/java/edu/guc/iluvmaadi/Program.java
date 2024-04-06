package edu.guc.iluvmaadi;

import java.util.Hashtable;
import java.util.Iterator;

public class Program {
    public static void main( String[] args ){

        try{
            DBApp	dbApp = new DBApp( );

            //System.out.println(dbApp.maximumRowsCountinPage);

            String strTableName = "Student";
            if (dbApp.getTable(strTableName) == null) {
                Hashtable<String, String> htblColNameType = new Hashtable<>( );
                htblColNameType.put("id", "java.lang.Integer");
                htblColNameType.put("name", "java.lang.String");
                htblColNameType.put("gpa", "java.lang.Double");
                dbApp.createTable( strTableName, "id", htblColNameType );

                dbApp.createIndex( strTableName, "gpa", "gpaIndex");
            }

//            dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

//            for (int i = -20; i <= 1; i++) {
//                Hashtable<String, Object> htblColNameValue = new Hashtable<>( );
//                htblColNameValue.put("id", i );
//                htblColNameValue.put("name", "John Noor" + i );
//                htblColNameValue.put("gpa", 1.5 );
//                dbApp.insertIntoTable( strTableName , htblColNameValue );
//            }


            SQLTerm[] arrSQLTerms;
            arrSQLTerms = new SQLTerm[3];
            arrSQLTerms[0] = new SQLTerm("Student", "name", ">=", "John Noor-10");
            arrSQLTerms[1] = new SQLTerm("Student", "name", "<", "John Noor-15");
            arrSQLTerms[2] = new SQLTerm("Student", "gpa", "<", 2.0);

            String[]strarrOperators = new String[2];
            strarrOperators[0] = "AND";
            strarrOperators[1] = "AND";

            Iterator<Tuple> iterator = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                System.out.println(tuple);
            }
        }
        catch(Exception exp){
            exp.printStackTrace( );
        }
    }
}
