package com.cac.restfull.database;

import java.util.List;
import javax.json.Json;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import java.io.ByteArrayInputStream;
import com.cac.restfull.webservice.Util;
import javax.validation.constraints.NotNull;
import static com.cac.restfull.database.DBConnection.getDBConnection;
import java.sql.Types;

/**
 * Clase encargada de resolver las peticiones al servidor.
 *
 * @author Administrator
 */
public class DBServiceManager {

    private static final Logger LOG = Logger.getLogger(DBServiceManager.class.getName());

    /**
     * Metodo utilizado para conocer si el servidor se encuentra conectado a la
     * base de datos.
     *
     * @return true en caso de coneccion valida, false en caso contrario.
     *
     */
    public synchronized boolean isDBConected() {
        // Test query.
        String query = "select 'connected' AS DUMMY from dual";
        Statement st = null;

        try {

            st = getDBConnection().createStatement();
            ResultSet rs = st.executeQuery(query);

            rs.next();

            return rs.getString("DUMMY").equalsIgnoreCase("connected");

        } catch (Exception e) {
            Logger.getLogger(DBConnection.class.getName()).log(Level.SEVERE, "Error al probar la consulta", e);
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException ex) {
                    LOG.log(Level.SEVERE, "Error al cerrar el statement", ex);
                }
            }
        }
        return false;
    }

    /**
     * Metodo utilizado para conseguir la informacion de una tabla en
     * especifico.
     *
     * @param tableName nombre de la tabla a buscar.
     * @param columns columnas a buscar.
     * @throws Exception posible exception en caso de que una de las columnas no
     * exista en la Base de datos.
     * @return string con la informacion seleccionada. Formato de la respuesta:
     * { "tag" : "response", "status" : "true", "tableName" : "table name",
     * "rows" : JsonArray[JsonObject{"key":"value","key1":"value1"}]}
     */
    public synchronized String getDataFromTable(String tableName, JsonArray columns) throws Exception {

        //Formating columns
        String formatedColumns = "";

        for (int i = 0; i < columns.size(); i++) {
            formatedColumns += columns.getString(i);

            if (i < (columns.size() - 1)) {
                formatedColumns += ",";
            }
        }

        // Parametros obligatorios del query.
        String sql = "select " + formatedColumns + " from " + tableName;

        LOG.log(Level.SEVERE, "SQL {0}", sql);

        PreparedStatement ps = null;

        try {
            // Conseguimos la coneccion y creamos el prepare statement
            ps = getDBConnection().prepareStatement(sql);

            //Ejecutamos el query
            ResultSet rs = ps.executeQuery();

            //Formamos el atributo rows
            JsonArrayBuilder rows = Json.createArrayBuilder();
            while (rs.next()) {
                JsonObjectBuilder objBuilder = Json.createObjectBuilder();
                for (int j = 0; j < columns.size(); j++) {
                    objBuilder.add(columns.getString(j), rs.getString(columns.getString(j)));
                }
                rows.add(objBuilder.build());
            }

            JsonObjectBuilder objectToSend = Util.createJSONObjectBuilder("response", true);
            objectToSend.add("tableName", tableName);
            objectToSend.add("rows", rows.build());

            String json = objectToSend.build().toString();

            LOG.log(Level.INFO, "===============================================");
            LOG.log(Level.INFO, "Response {0}", json);
            LOG.log(Level.INFO, "===============================================");

            return json;

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            throw ex;
        } finally {
            if (ps != null) {
                ps.close();
            }
        }

    }

    /**
     * Metodo utilizado para buscar la informacion de una tabla en la base de 
     * datos, segun las condiciones solicitadas por el usuario.
     * 
     * @param tableName : Nombre de la tabla a buscar.
     * @param columns   : Columnas a seleccionar.
     * @param whereCondition : Condicion del select, ejemplo: empresa = ?
     * @param whereValues : Valores de las condicion del select.
     * @exception Exception : Posible exception al buscar en la base de datos.
     * @return JsonObject con la respuesta del servidor. 
     * Ejemplo del formato:
     * { "tag"       : "response",
     *     "status"    : "true",
     *     "tableName" : "table name",
     *     "rows"      : JsonArray[JsonObject{"key":"value","key1":"value1"}]}
     */
    public synchronized String getDataFromTable(@NotNull String tableName, @NotNull JsonArray columns, String whereCondition, JsonArray whereValues) throws Exception {

        //Formating columns
        String formatedColumns = "";

        for (int i = 0; i < columns.size(); i++) {
            formatedColumns += columns.getString(i);

            if (i < (columns.size() - 1)) {
                formatedColumns += ",";
            }
        }

        // Parametros obligatorios del query.
        String sql = "select " + formatedColumns + " from " + tableName;

        // En caso de que el query tenga alguna condicion.
        if (!whereCondition.isEmpty()) {
            sql += " where " + whereCondition;
        }

        LOG.log(Level.INFO, "SQL {0}", sql);

        PreparedStatement ps = null;

        try {
            // Conseguimos la coneccion y creamos el prepare statement
            ps = getDBConnection().prepareStatement(sql);

            //Setiamos los valores del where condition
            if (whereValues != null) {
                for (int i = 0; i < whereValues.size(); i++) {
                    ps.setString(i + 1, whereValues.get(i).toString().replaceAll("\"", ""));
                }
            }

            //Ejecutamos el query
            ResultSet rs = ps.executeQuery();

            //Formamos el atributo rows
            JsonArrayBuilder rows = Json.createArrayBuilder();
            while (rs.next()) {
                JsonObjectBuilder objBuilder = Json.createObjectBuilder();
                for (int j = 0; j < columns.size(); j++) {
                    if (columns.getString(j).contains("fecha")) {
                        long lg = rs.getDate(columns.getString(j)).getTime();
                        objBuilder.add(columns.getString(j), String.valueOf(lg));
                    } else {
                        String value = rs.getString(columns.getString(j));
                        objBuilder.add(columns.getString(j), value == null ? " " : value);
                    }
                }
                rows.add(objBuilder.build());
            }

            JsonObjectBuilder objectToSend = Util.createJSONObjectBuilder("response", true);
            objectToSend.add("tableName", tableName);
            objectToSend.add("rows", rows.build());

            String json = objectToSend.build().toString();

            LOG.log(Level.INFO, "===============================================");
            LOG.log(Level.INFO, "Response {0}", json);
            LOG.log(Level.INFO, "===============================================");

            return json;

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            throw ex;
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    /**
     * Metodo utilizado para permitir insertar informacion en la base de datos 
     * de oracle.
     * 
     * @return Respuesta del servidor.
     * @exception Exception Posible error al insertar en la base de datos.
     * @param obj : Informacion a insertar en la base datos.
     * Formato del objeto.
     * {"rows" : JsonArray[JsonObject {"tableName" : "table name", "columns":
     * JsonArray[column1, column2, n...],
     * "values":JsonArray[JsonObject{"type":"INTEGER","VALUE":"value"},
     * JsonObject{"type":"STRING","VALUE":"value"}, n...]} ,{n..}] }
     */
    public String insertDataIntoTable(JsonObject obj) throws Exception {

        JsonArray rows = obj.getJsonArray("rows");

        if (rows == null) {
            throw new Exception("Json Malformed." + rows.toString());
        } else {
            //Iterating Json.
            for (int i = 0; i < rows.size(); i++) {
                //registros
                JsonObject row = rows.getJsonObject(i);

                //Formando Registros
                String sql = "insert into " + row.getString("tableName");
                sql += " (";

                // working with columns
                JsonArray columns = row.getJsonArray("columns");
                for (int j = 0; j < columns.size(); j++) {
                    sql += columns.getString(j);
                    if (j < (columns.size() - 1)) {
                        sql += ",";
                    }
                }
                sql += " ) values ( ";

                // working with values
                JsonArray values = row.getJsonArray("values");
                List<WrapperValues> detailsValues = new ArrayList<>();
                for (int j = 0; j < values.size(); j++) {

                    JsonObject detailValuePair = values.getJsonObject(j);
                    WrapperValues detailValue = new WrapperValues();
                    detailValue.setSortField(j + 1);
                    detailValue.setFieldValue(detailValuePair.getString("value"));
                    detailValue.setFieldType(getFiledTypeFromString(detailValuePair.getString("type")));

                    detailsValues.add(detailValue);

                    sql += "?";
                    if (j < (values.size() - 1)) {
                        sql += ",";
                    }
                }

                sql += " )";

                //list of values
                LOG.log(Level.SEVERE, "SQL {0}", sql);

                insertDataIntoTable(sql, detailsValues);

            }
            return Util.createJSON("response", true);
        }

    }

    /**
     * Metodo utilizado para obtener el tipo de dato de la columna.
     * 
     * @param fieldType : Tipo de dato a convertir.
     * @return Tipo de dato segun el enum de FieldType.
     */
    private FieldType getFiledTypeFromString(String fieldType) throws Exception {
        if (fieldType.equalsIgnoreCase(FieldType.DATE.toString())) {
            return FieldType.DATE;
        } else if (fieldType.equalsIgnoreCase(FieldType.STRING.toString())) {
            return FieldType.STRING;
        } else if (fieldType.equalsIgnoreCase(FieldType.FLOAT.toString())) {
            return FieldType.FLOAT;
        } else if (fieldType.equalsIgnoreCase(FieldType.INTEGER.toString())) {
            return FieldType.INTEGER;
        } else if (fieldType.equalsIgnoreCase(FieldType.BLOB.toString())) {
            return FieldType.BLOB;
        } else {
            throw new Exception("Unsupported Value: " + fieldType);
        }
    }

    /**
     * Metodo utilizado para insertar un registro en la base de datos.
     * 
     * @param sql Query para insert la infomacion 
     * ejemplo: insert into tableName(campo1, campo2) values (?,?)
     * @param values listado des de valores a insertar. Estos parametro seran 
     * reemplazados por los ?.
     * @exception SQLException Posible error al insertar en la base de datos.
     */
    private synchronized void insertDataIntoTable(String sql, List<WrapperValues> values) throws SQLException {

        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = getDBConnection().prepareStatement(sql);

            for (WrapperValues wv : values) {
                switch (wv.getFieldType()) {
                    case INTEGER:
                        if ( !wv.getFieldValue().equalsIgnoreCase("null") ){
                            preparedStatement.setInt(wv.getSortField(), Integer.parseInt(wv.getFieldValue()));
                        } else {
                            preparedStatement.setNull(wv.getSortField(), Types.INTEGER);
                        }
                        break;
                    case STRING:
                        if ( !wv.getFieldValue().equalsIgnoreCase("null") ){
                            preparedStatement.setString(wv.getSortField(), wv.getFieldValue());
                        } else {
                            preparedStatement.setNull(wv.getSortField(), Types.VARCHAR);
                        }
                        break;
                    case DATE:
                        // always is long, must convert long to timeestamp
                        String cadena = wv.getFieldValue();
                        if ( !cadena.equalsIgnoreCase("null") ) {
                            Long valor = Long.parseLong(cadena);
                            Timestamp tiempo = new Timestamp(valor);
                            preparedStatement.setTimestamp(wv.getSortField(), tiempo);
                        } else {
                            preparedStatement.setNull(wv.getSortField(),Types.DATE);
                        }
                        break;
                    case FLOAT:
                        if ( !wv.getFieldValue().equalsIgnoreCase("null") ) {
                            preparedStatement.setFloat(wv.getSortField(), Float.parseFloat(wv.getFieldValue()));
                        } else {
                            preparedStatement.setNull(wv.getSortField(), Types.FLOAT);
                        }
                        break;
                    case BLOB:
                        if ( wv.getFieldValue().equalsIgnoreCase("null") ) {
                            byte[] byteArray = Util.decodeImage(wv.getFieldValue());
                            preparedStatement.setBinaryStream(wv.getSortField(), new ByteArrayInputStream(byteArray), byteArray.length);
                        } else {
                            preparedStatement.setNull(wv.getSortField(), Types.BLOB);
                        }
                        break;
                    default:
                        throw new SQLException("Unsupported type of field. Type: " + wv.getFieldType() + " Value: " + wv.getFieldValue());
                }
            }

            //execute sql
            int result = preparedStatement.executeUpdate();

            LOG.log(Level.INFO, "==============================================");
            LOG.log(Level.INFO, "Registros creados {0}", preparedStatement.getUpdateCount());
            LOG.log(Level.INFO, "==============================================");

        }  catch (Exception ex) {
            LOG.log(Level.SEVERE, "Ocurrio un error al insertDataIntoTable", ex);
            throw new SQLException(ex);
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    //==========================================================================
    // METODOS CREADOS PARA INSERTAR LA ORDEN DE TRABAJO
    //==========================================================================
    public String insertOrdenTrabajo(JsonObject obj) throws Exception {

        JsonObject encabezado = obj.getJsonObject("encabezado");
        JsonArray detalle     = obj.getJsonArray("detalle");
        JsonArray adjuntos    = obj.getJsonArray("adjuntos");
        JsonObject parametros = obj.getJsonObject("parametros");

        if (encabezado == null || detalle == null) {
            throw new Exception("Json Malformed." + encabezado.toString());
        } else {
            
            int empresa, area, tipoOT;
            empresa = Integer.parseInt(parametros.getString("empresa"));
            area    = Integer.parseInt(parametros.getString("area"));
            tipoOT  = Integer.parseInt(parametros.getString("tipo_ot"));
            
            String noOrdenTrabajo = getNoOrdenTrabajo(empresa);
            String formatoOrdenTrabajo = getFormatoOrdenTrabajo(empresa,
                    area, tipoOT);
            crearInsertOrdenTrabajo(encabezado, noOrdenTrabajo, formatoOrdenTrabajo);
            for (int i = 0; i < detalle.size(); i++) {
                crearInsertOrdenTrabajo(detalle.getJsonObject(i),
                        noOrdenTrabajo, formatoOrdenTrabajo);
            }
            if (adjuntos != null) {
                for (int i = 0; i < detalle.size(); i++) {
                    crearInsertOrdenTrabajo(adjuntos.getJsonObject(i),
                            noOrdenTrabajo, formatoOrdenTrabajo);
                }
            }
            JsonObjectBuilder response = Util.createJSONObjectBuilder("response", true);
            response.add("noOrdenTrabajo", noOrdenTrabajo);
            response.add("correlativo_ot", formatoOrdenTrabajo);

            return response.build().toString();
        }
    }

    /**
     * Metodo utilizado para convertir un Json en un insert de oracle. El
     * JsonObject debe tener el siguiente formato. JsonObject {
     * tableName:"ma_maestro_orden_trabajo", columns : JsonArray[id_empresa,
     * id_periodo,...], values :
     * JsonArray[JsonObject{"type":"INTEGER","VALUE":"value"}] }
     *
     * @param obj : JsonObject con el formato indicado, con los datos y la tabla
     * a insertar.
     * @param campos: Campos a reemplazar con nombre y valor.
     * @return Mapa con los objetos necesarios para realizar el insert, SQL y
     * valores.
     */
    private void crearInsertOrdenTrabajo(JsonObject obj, String ordenTrabajo, String formatoOrdeTrabajo) throws Exception {
        
        LOG.log(Level.SEVERE,"=============================================");
        LOG.log(Level.SEVERE,"JSON: "+obj.toString());
        LOG.log(Level.SEVERE,"=============================================");
        
        // Formando Registros
        String sql = "insert into " + obj.getString("tableName");
        sql += " (";

        int noOrdenTrabajo = 0;
        int correlativoOT = 0;

        // Working with columns
        JsonArray columns = obj.getJsonArray("columns");
        for (int j = 0; j < columns.size(); j++) {
            sql += columns.getString(j);
            if (columns.getString(j).equalsIgnoreCase("no_orden_trabajo")) {
                noOrdenTrabajo = j;
            } else if (columns.getString(j).equalsIgnoreCase("correlativo_ot")) {
                correlativoOT = j;
            }
            if (j < (columns.size() - 1)) {
                sql += ",";
            }
        }
        sql += " ) values ( ";

        // working with values
        JsonArray values = obj.getJsonArray("values");
        List<WrapperValues> detailsValues = new ArrayList<>();
        for (int j = 0; j < values.size(); j++) {

            JsonObject detailValuePair = values.getJsonObject(j);
            WrapperValues detailValue = new WrapperValues();
            detailValue.setSortField(j + 1);
            if (noOrdenTrabajo > 0 && j == noOrdenTrabajo) {
                detailValue.setFieldValue(ordenTrabajo);
            } else if (correlativoOT > 0 && correlativoOT == j) {
                detailValue.setFieldValue(formatoOrdeTrabajo);
            } else {
                detailValue.setFieldValue(detailValuePair.getString("value"));
            }
            detailValue.setFieldType(getFiledTypeFromString(detailValuePair.getString("type")));

            detailsValues.add(detailValue);

            sql += "?";
            if (j < (values.size() - 1)) {
                sql += ",";
            }
        }

        sql += " )";
        
        LOG.log(Level.SEVERE,"SQL : "+sql);
        
        insertDataIntoTable(sql, detailsValues);
    }

    private String getNoOrdenTrabajo(int empresa) throws Exception {
        PreparedStatement preparedStatement = null;

        String sql = "SELECT NVL(MAX(NVL(NO_ORDEN_TRABAJO,0)),0) + 1 CORRELATIVO \n"
                + "FROM MAQUINARIAN.MA_MAESTRO_ORDEN_TRABAJO\n"
                + "WHERE ID_EMPRESA = " + empresa;
        try {
            preparedStatement = getDBConnection().prepareCall(sql);
            ResultSet result = preparedStatement.executeQuery();
            result.next();

            return result.getString("CORRELATIVO");

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Ocurrio un error al getNoOrdenTrabajo", e);
            throw new SQLException(e);
        }
    }

    private String getFormatoOrdenTrabajo(int empresa, int area, int tipoOt) throws Exception {
        try {

            String call = "{ ? = call maquinarian.MA_FN_CORRELATIVO_OT(?,?,?) }";
            CallableStatement function = getDBConnection().prepareCall(call);
            function.registerOutParameter(1, oracle.jdbc.OracleTypes.VARCHAR);
            function.setInt(2, empresa);
            function.setInt(3, area);
            function.setInt(4, tipoOt);
            function.executeQuery();
            return function.getString(1);

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Ocurrio un error al getFormatoOrdenTrabajo", e);
            throw new SQLException(e);
        }
    }
    //==========================================================================
    // FIN ORDEN DE TRABAJO
    //==========================================================================

    private class WrapperValues {

        private int sortField;
        private String fieldValue;
        private FieldType fieldType;

        public WrapperValues(int sortField, String fieldValue, FieldType fieldType) {
            this.sortField = sortField;
            this.fieldValue = fieldValue;
            this.fieldType = fieldType;
        }

        public WrapperValues() {
        }

        public int getSortField() {
            return sortField;
        }

        public void setSortField(int sortField) {
            this.sortField = sortField;
        }

        public String getFieldValue() {
            return fieldValue;
        }

        public void setFieldValue(String fieldValue) {
            this.fieldValue = fieldValue;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        public void setFieldType(FieldType fieldType) {
            this.fieldType = fieldType;
        }
    }

    enum FieldType {
        INTEGER, DATE, STRING, FLOAT, BLOB
    }

}
