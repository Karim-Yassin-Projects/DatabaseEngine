package edu.guc.iluvmaadi;

/** * @author Wael Abouelsaadat */

public class DBAppException extends Exception {

    public DBAppException(String strMessage, Throwable e){
        super(strMessage, e);
    }

    public DBAppException( String strMessage ){
        super( strMessage );
    }
}