package com.bbd.IndicService.tool;


import java.io.InputStream;
import java.util.*;

public class PropertiesTool {
    public static String getPropertyValue(String name,String filename){
        Properties paramProp = new Properties();
        InputStream inputStream = PropertiesTool.class.getResourceAsStream("/"+filename);
        String values = null;
        try{
            paramProp.load(inputStream);
            values = paramProp.getProperty(name);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{inputStream.close();}catch (Exception e){}
        }
        return values;
    }

}
