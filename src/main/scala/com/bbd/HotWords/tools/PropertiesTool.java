package com.bbd.HotWords.tools;

import org.apache.kafka.common.serialization.StringDeserializer;

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

    public static Map<String,Object> getKafkaParamsMap(String fileName){
        Properties paramProp = new Properties();
        InputStream inputStream = PropertiesTool.class.getResourceAsStream("/"+fileName);
        Map<String,Object> map = new HashMap<String, Object>();
        try{
            paramProp.load(inputStream);
            Set<Map.Entry<Object,Object>> set = paramProp.entrySet();
            Iterator itr = set.iterator();
            while(itr.hasNext()){
                Map.Entry<Object,Object> entry = (Map.Entry<Object,Object>)itr.next();
                map.put((String)entry.getKey(),(String)entry.getValue());
            }
//            map.put("key.deserializer", StringDeserializer.class);
//            map.put("value.deserializer",StringDeserializer.class);
//            map.put("enable.auto.commit",false);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{inputStream.close();}catch (Exception e){}
        }
        return map;
    }
}
