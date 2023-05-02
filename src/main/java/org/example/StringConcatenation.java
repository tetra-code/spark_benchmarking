package org.example;

public class StringConcatenation {
    public static String simpleAppend(String s, int times){
        String string = "";
        for (int i=0; i<times; i++){
            string += s;
        }
        return string;
    }

    public static String appendWithBuilder(String s, int times){
        StringBuilder sb = new StringBuilder(times);
        for (int i=0; i<times; i++){
            sb.append(s);
        }
        return sb.toString();
    }
}