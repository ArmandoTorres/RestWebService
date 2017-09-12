package com.cac.restfull.webservice;

import com.sun.xml.wss.impl.misc.Base64;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.apache.xml.security.exceptions.Base64DecodingException;

/**
 *
 * @author Administrator
 */
public class Util {
    
    public static String createJSON(String tag, boolean status ){
        JsonObjectBuilder json = Json.createObjectBuilder();
        try {
            json.add("tag", tag);
            json.add("status", new Boolean(status));
        } catch (Exception e) {
        }
        return json.build().toString();
    }
    
    public static String createJSON(String tag, boolean status, String errorMsj){
        JsonObjectBuilder json = Json.createObjectBuilder();
        try {
            json.add("tag", tag);
            json.add("status", new Boolean(status));
            json.add("error", errorMsj);
        } catch (Exception e) {
        }
        return json.build().toString();
    }
    
    public static JsonObject createJSONObject(String tag, boolean status ){
        JsonObjectBuilder json = Json.createObjectBuilder();
        try {
            json.add("tag", tag);
            json.add("status", new Boolean(status));
        } catch (Exception e) {
        }
        return json.build();
    }
    
    public static JsonObject createJSONObject(String tag, boolean status, String errorMsj){
        JsonObjectBuilder json = Json.createObjectBuilder();
        try {
            json.add("tag", tag);
            json.add("status", new Boolean(status));
            json.add("error", errorMsj);
        } catch (Exception e) {
        }
        return json.build();
    }
    
    public static JsonObjectBuilder createJSONObjectBuilder(String tag, boolean status){
        JsonObjectBuilder json = Json.createObjectBuilder();
        try {
            json.add("tag", tag);
            json.add("status", new Boolean(status));
        } catch (Exception e) {
        }
        return json;
    }
    
    public static String encodeImage(byte[] imageByteArray){
        return Base64.encode(imageByteArray);
        //return Base64.encodeBase64URLSafeString(imageByteArray);
    }
    
    public static byte[] decodeImage( String imageDataString ) throws Base64DecodingException  {
        return Base64.decode(imageDataString);
    }
    
}
