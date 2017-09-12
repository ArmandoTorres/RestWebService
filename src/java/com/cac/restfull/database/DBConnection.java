package com.cac.restfull.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.util.logging.Logger;
import com.cac.restfull.webservice.AppConstant;

/**
 * Clase encargada de gestionar la comunicacion con la base de datos oracle.
 * 
 * @author ATORRES
 */
public class DBConnection {
    
    private static Connection connection = null;
    private static final Logger LOG = Logger.getLogger(DBConnection.class.getName());
    
    private DBConnection() {}
    
    public static Connection getDBConnection() throws ClassNotFoundException, SQLException {
        if ( connection == null ){
            //Cargamos el driver.
            Class.forName(AppConstant.DB_CLASS);
            //Accedemos a la base de datos.
            connection = DriverManager.getConnection(AppConstant.URL,AppConstant.USER,AppConstant.PASS);
        }
        return connection;
    }
}