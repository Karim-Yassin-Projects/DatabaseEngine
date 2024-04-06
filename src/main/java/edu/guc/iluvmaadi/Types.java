package edu.guc.iluvmaadi;

import java.util.Arrays;

public class Types {

    public static final String[] validTypes = {
            "java.lang.Double",
            "java.lang.Integer",
            "java.lang.String",
    };

    public static boolean isValidColType(String type) {
        return Arrays.binarySearch(validTypes, type) >= 0;
    }
}
