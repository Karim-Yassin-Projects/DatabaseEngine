package edu.guc.iluvmaadi;

import java.io.Serial;
import java.io.Serializable;

public class PageInfo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Comparable<Object> min;
    private Comparable<Object> max;
    private final int pageNumber;
    private int size;

    public PageInfo(int pageNumber, Comparable<Object> min, Comparable<Object> max) {
        this.pageNumber = pageNumber;
        this.min = min;
        this.max = max;
    }

    public Comparable<Object> getMin() {
        return min;
    }

    public void setMin(Comparable<Object> min) {
        this.min = min;
    }

    public Comparable<Object> getMax() {
        return max;
    }

    public void setMax(Comparable<Object> max) {
        this.max = max;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isFull() {
        return size == DBApp.maximumRowsCountinPage;
    }

    @Override
    public String toString() {
        return "PageInfo{" + "min=" + min + ", max=" + max + ", pageNumber=" + pageNumber + ", size=" + size + '}';
    }
}
