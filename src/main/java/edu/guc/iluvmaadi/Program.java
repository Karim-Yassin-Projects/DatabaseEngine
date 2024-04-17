package edu.guc.iluvmaadi;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

public class Program {

    private static final Random random = new Random(9000000);

    private static void createStudentTable(DBApp dbApp) throws DBAppException {
        String strTableName = "Student";
        if (dbApp.getTable(strTableName) == null) {
            Hashtable<String, String> htblColNameType = new Hashtable<>();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable(strTableName, "id", htblColNameType);

            dbApp.createIndex(strTableName, "gpa", "gpaIndex");
        }
    }

    private static void insertStudent(int i, DBApp dbApp, String studentTableName) throws DBAppException {

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", i);
        htblColNameValue.put("name", "Ahmed Noor-" + i);
        htblColNameValue.put("gpa", (double) Math.round(random.nextDouble() * 43 + 7) / 10);
        dbApp.insertIntoTable(studentTableName, htblColNameValue);
    }

    private static void insertStudents(DBApp dbApp, int startId, int numberOfStudents) throws DBAppException {
        System.out.println("--------------------");
        System.out.println("Inserting " + numberOfStudents + " students");

        String studentTableName = "Student";
        for (int i = 0; i < numberOfStudents; i++) {
            insertStudent(startId + i, dbApp, studentTableName);
        }

        System.out.println("Inserted " + numberOfStudents + " students");
        System.out.println("--------------------");
    }

    public static void selectAll(DBApp dbApp, String studentTableName) throws DBAppException {
        System.out.println();
        System.out.println("--------------------");
        System.out.println("Selecting all students");

        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm(studentTableName, "id", "=", 0);
        arrSQLTerms[1] = new SQLTerm(studentTableName, "id", "!=", 0);
        String[] strarrOperators = new String[1];
        strarrOperators[0] = "OR";

        int count = 0;
        Iterator<Tuple> iterator = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            System.out.println(tuple);
            count++;
        }

        System.out.println();
        System.out.println("Select all done returned " + count + " rows");
        System.out.println("--------------------");
    }

    public static void selectCountAll(DBApp dbApp, String studentTableName) throws DBAppException {

        System.out.println("--------------------");
        System.out.println("Selecting all students");


        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm(studentTableName, "id", "=", 0);
        arrSQLTerms[1] = new SQLTerm(studentTableName, "id", "!=", 0);
        String[] strarrOperators = new String[1];
        strarrOperators[0] = "OR";

        Iterator<Tuple> iterator = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        int count = 0;
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            count++;
        }
        System.out.println();
        System.out.println("Select all done returned " + count + " rows");
        System.out.println("--------------------");
    }

    public static void selectStudentWithGpaRange(DBApp dbApp, String studentTableName, double minGpa, double maxGpa) throws DBAppException {
        System.out.println("--------------------");
        System.out.println("Selecting students with gpa between " + minGpa + " and " + maxGpa);
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0] = new SQLTerm("Student", "gpa", ">=", minGpa);
        arrSQLTerms[1] = new SQLTerm("Student", "gpa", "<=", maxGpa);


        String[] strarrOperators = new String[1];
        strarrOperators[0] = "AND";

        int count = 0;
        Iterator<Tuple> iterator = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            System.out.println(tuple);
            count++;
        }

        System.out.println("There are " + count + " students with gpa between " + minGpa + " and " + maxGpa);
        System.out.println("--------------------");
    }

    public static void selectStudentWithMinGpa(DBApp dbApp, String studentTableName, double minGpa) throws DBAppException {
        System.out.println("--------------------");
        System.out.println("Selecting students with gpa >= " + minGpa);
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm("Student", "gpa", ">=", minGpa);


        String[] strarrOperators = new String[0];

        int count = 0;
        Iterator<Tuple> iterator = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            System.out.println(tuple);
            count++;
        }

        System.out.println("There are " + count + " students with gpa > " + minGpa);
        System.out.println("--------------------");
    }

    public static void deleteStudentsWithGpa(DBApp dbApp, String studentTableName, double gpa) throws DBAppException {
        System.out.println("--------------------");
        System.out.println("Deleting students with gpa " + gpa);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("gpa", gpa);

        dbApp.deleteFromTable(studentTableName, htblColNameValue);

        System.out.println("Deleted students with gpa " + gpa);
    }

    public static void updateStudentName(DBApp dbApp, String studentTableName, int id, String newName) throws DBAppException {
        System.out.println("--------------------");
        System.out.println("Updating student with id " + id + " to " + newName);
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("name", newName);
        dbApp.updateTable(studentTableName, Integer.toString(id), htblColNameValue);
        System.out.println("Updated student with id " + id + " to " + newName);
        System.out.println("--------------------");
    }

    public static void deleteAllStudents(DBApp dbApp, String studentTableName) throws DBAppException {
        System.out.println("--------------------");
        System.out.println("Deleting all students");
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        dbApp.deleteFromTable(studentTableName, htblColNameValue);
        System.out.println("Deleted all students");
        System.out.println("--------------------");
    }

    public static void selectStudentWithId(DBApp dbApp, String studentTableName, int id) throws DBAppException {
        System.out.println("--------------------");
        System.out.println("Selecting student with id " + id);
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[1];
        arrSQLTerms[0] = new SQLTerm(studentTableName, "id", "=", id);

        String[] strarrOperators = new String[0];

        Iterator<Tuple> iterator = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        int count = 0;
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            System.out.println(tuple);
            count++;
        }

        System.out.println("There are " + count + " students with id " + id);
        System.out.println("--------------------");
    }



    public static void main(String[] args) {


        try {
            DBApp dbApp = new DBApp();

            String studentTableName = "Student";
            createStudentTable(dbApp);
            deleteAllStudents(dbApp, studentTableName);
            selectAll(dbApp, studentTableName);

            insertStudents(dbApp, 1, 200);
            selectAll(dbApp, studentTableName);

            selectStudentWithId(dbApp, studentTableName, 40);
            updateStudentName(dbApp, studentTableName, 40, "Kareem Noor-40 Updated");
            selectStudentWithId(dbApp, studentTableName, 40);


            selectStudentWithGpaRange(dbApp, studentTableName, 2.4, 2.6);
            deleteStudentsWithGpa(dbApp, studentTableName, 2.5);
            selectStudentWithGpaRange(dbApp, studentTableName, 2.4, 2.6);

            selectStudentWithMinGpa(dbApp, studentTableName, 3);

            selectCountAll(dbApp, studentTableName);

        } catch (Exception exp) {
            //noinspection CallToPrintStackTrace
            exp.printStackTrace();
        }
    }


}
