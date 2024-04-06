package edu.guc.iluvmaadi;

import java.io.*;
import java.util.Vector;

public class TableInfo implements java.io.Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final Vector<PageInfo> pagesInfo = new Vector<>();
    private int nextPageNumber = 0;
    private String tableName;

    public TableInfo(String tableName) {
        this.tableName = tableName;
    }

    public void saveTableInfo() throws DBAppException {
        File file = new File(getTablePath(tableName));
        File parent = file.getParentFile();
        if (!parent.exists()) {
            parent.mkdirs();
        }
        try (FileOutputStream fileOutputStream = new FileOutputStream(file.getAbsoluteFile());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(this);
        } catch (IOException e) {
            throw new DBAppException("Error saving table " + tableName, e);
        }
    }

    public static TableInfo loadTableInfo(String tableName) throws DBAppException {
        File file = new File(getTablePath(tableName));
        if (!file.exists()) {
            return new TableInfo(tableName);
        }

        try (FileInputStream fileInputStream = new FileInputStream(file.getAbsoluteFile());
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            TableInfo tableInfo = (TableInfo) objectInputStream.readObject();
            return tableInfo;

        } catch (IOException | ClassNotFoundException e) {
            throw new DBAppException("Error saving table " + tableName, e);
        }
    }

    private String getTablePath() {
        return getTablePath(tableName);
    }

    public static String getTablePath(String tableName) {
        return "DBEngine-Data" + "/" + tableName + ".class";
    }

    public Vector<PageInfo> getPagesInfo() {
        return pagesInfo;
    }

    public int getNextPageNumber() {
        return nextPageNumber++;
    }

}
