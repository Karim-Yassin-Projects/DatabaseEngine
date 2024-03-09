package edu.guc.iluvmaadi;

import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable, Comparable<Tuple> {
    private Vector<Comparable<Object>> values;
    public Tuple(){
        values = new Vector<>();
    }
    public Vector<Comparable<Object>> getValues(){
        return values;
    }

    public Comparable<Object> getKey(){
        return values.getFirst();
    }

    @Override
    public int compareTo(Tuple o) {
        return values.getFirst().compareTo(o.getKey());
    }

    public String toString(){
        String res = "";
        for(int i = 0; i < values.size()-1; i++){
            res += values.get(i) + ",";
        }
        res += values.get(values.size()-1);
        return res;
    }


}
