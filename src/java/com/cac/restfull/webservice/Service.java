package com.cac.restfull.webservice;

import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import com.cac.restfull.database.DBServiceManager;
import javax.ws.rs.QueryParam;

/**
 * Clase encargada de proveeer los servicios manejados por el webservice.
 *
 *
 * @author atorres
 */
@Path("/Service")
public class Service {

    private static final Logger LOG = Logger.getLogger(Service.class.getName());

    private DBServiceManager dBServiceManager = null;

    public Service() {
        dBServiceManager = new DBServiceManager();
    }

    /**
     * Metodo utilizado para saber si el servicdor se encuentra conectado a la
     * base de datos.
     *
     * @return String true si esta conectado, false en caso contrario.
     */
    @GET
    @Path("/isOnline")
    @Produces(MediaType.APPLICATION_JSON)
    public String isOnline() {
        return Util.createJSON("response", dBServiceManager.isDBConected());
    }

    /**
     * Metodo utilizado para devolver en formato json los registros contenidos,
     * en la base de datos segun los especificado por el cliente.
     *
     * @param select : Json con la peticion de los registros contenidos en una
     * tabla. segun el formato. {"tableName":"table name", "columns":
     * JsonArray[column1, column2, n...], "where" : "condition1 = ? and
     * condition2 = ? ...", "whereValues" : JsonArray[ParamValue1, ParamValue2,
     * n...]}
     * @return Json : Respuesta con los registros obtenidos de la base de datos
     *   { "tag"       : "response",
           "status"    : "true",
           "tableName" : "table name",
           "rows"      : JsonArray[JsonObject{"key":"value","key1":"value1"}]}
     */
    @GET
    @Path("/getDataFromTable")
    @Produces(MediaType.APPLICATION_JSON)
    public String getDataFromTable(@QueryParam("select") String select) {
        try {
            JsonReader reader = Json.createReader(new StringReader(select));
            JsonObject obj = reader.readObject();
            if (!obj.toString().equals("{}")) {
                String tableName = obj.getString("tableName");
                JsonArray columns = obj.getJsonArray("columns");
                if (obj.containsKey("whereCondition")) {
                    String whereCondition = obj.getString("whereCondition");
                    JsonArray whereValues = obj.getJsonArray("whereValues");
                    return dBServiceManager.getDataFromTable(tableName, columns, whereCondition, whereValues);
                }
                return dBServiceManager.getDataFromTable(tableName, columns);
            } else {
                return Util.createJSON("response", false, "La peticion enviada esta vacia {}.");
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Ocurrio un error a la hora de resolver la peticion.", ex);
            return Util.createJSON("response", false, ex.getMessage());
        }
    }

    /**
     * Metodo utilizado para insertar en la base de datos la informacion
     * indicada por el cliente.
     *
     * @param insert : Json con la informacion a insertar. Segun el formato:
     * {"rows" : JsonArray[JsonObject {"tableName" : "table name", "columns":
     * JsonArray[column1, column2, n...],
     * "values":JsonArray[JsonObject{"type":"INTEGER","VALUE":"value"},
     * JsonObject{"type":"STRING","VALUE":"value"}, n...]} ,{n..}] }
     * 
     * @return Json : Respuesta con los registros obtenidos de la base de datos
     * {"tag" : "values", "status": true||false}
     */
    @GET
    @Path("/insertDataIntoTable")
    @Produces(MediaType.APPLICATION_JSON)
    public String insertDataIntoTable(@QueryParam("insert") String insert) {

        try {
            JsonReader reader = Json.createReader(new StringReader(insert));
            JsonObject obj = reader.readObject();
            return dBServiceManager.insertDataIntoTable(obj);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Ocurrio un error a la hora de resolver la peticion.", ex);
            return Util.createJSON("response", false, ex.getMessage());
        }
    }
    
    @GET
    @Path("/insertOrdenTrabajo")
    @Produces(MediaType.APPLICATION_JSON)
    public String insertOrdenTrabajo(@QueryParam("insert") String insert) {

        LOG.log(Level.INFO,"Request insertOrdenTrabajo : "+insert);
        try {
            JsonReader reader = Json.createReader(new StringReader(insert));
            JsonObject obj = reader.readObject();
            return dBServiceManager.insertOrdenTrabajo(obj);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Ocurrio un error a la hora de resolver la peticion.", ex);
            return Util.createJSON("response", false, ex.getMessage());
        }
    }
}
