package edu.guc.iluvmaadi;

import java.io.*;
import java.util.Vector;

public class Page implements Serializable {
    private int pageNumber;
    private Comparable<Object> max;
    private Comparable<Object> min;
    private final int size = DBApp.pageSize;
    private int currentSize;
    private Vector<Tuple> tuples;
    private final String tableName;



    public Page(int pageNumber, String tableName, Comparable<Object> max, Comparable<Object> min) {
        this.pageNumber = pageNumber;
        this.max = max;
        this.min = min;
        this.currentSize = 0;
        this.tuples = new Vector<>();
        this.tableName = tableName;


    }

    public int getPageNumber() {
        return pageNumber;
    }

    public Comparable<Object> getMax() {
        return max;
    }

    public Comparable<Object> getMin() {
        return min;
    }

    public int getSize() {
        return size;
    }

    public int getCurrentSize() {
        return currentSize;
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public String getTableName() {
        return tableName;
    }





    public void setMax(Comparable<Object> max) {
        this.max = max;
    }

    public void setMin(Comparable<Object> min) {
        this.min = min;
    }

    public void setCurrentSize(int currentSize) {
        this.currentSize = currentSize;
    }

    public int binarySearch(int key) { //index of the key{
        int low = 0;
        int high = currentSize - 1;
        while (high >= low) {
            int middle = (low + high) / 2;
            if (tuples.get(middle).getKey().compareTo(key) == 0) {
                return middle;
            }
            if (tuples.get(middle).getKey().compareTo(key) < 0) {
                low = middle + 1;
            }
            if (tuples.get(middle).getKey().compareTo(key) > 0) {
                high = middle - 1;
            }
        }
        return -1;
    }

    public void savePage(){
        String fileName = this.tableName + "_page" + this.pageNumber + ".ser";
        try(FileOutputStream fileOut = new FileOutputStream(fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut)){;
            out.writeObject(this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
