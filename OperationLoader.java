package OperLoader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;

/**
 *
 * @author VErin AnalytikaPlus
 */
public class OperationLoader {
    private static Connection oraConnection;     // Дескриптор соединения Oracle
    private static Connection verticaConnection; // Дескриптор соединения Vertica
    private static int rowCount;
    private static final String Version = "1.1.00";
    private static final String propertiesFileName = "loader.properties"; 
    private static final String Copyright = "Разработано \"Цифра Один\", 2016"; 
 
    private static Connection oraConnect(String username, String password, String server, String port, String sid) throws SQLException {
        String url;
        // Строка соединения для JDBC драйвера Oracle (thin)
        url = "jdbc:oracle:thin:@" + server + ":" + port + "/" + sid;
        return DriverManager.getConnection(url, username, password);
    }
    
    private static Connection verticaConnect(String username, String password, String server, String port, String dbName) throws SQLException {
        String url;
        // Строка соединения для JDBC драйвера Vertica
        url = "jdbc:vertica://"+server+":"+port+"/"+dbName;
        Properties verticaProp = new Properties();
        verticaProp.put("user", username); 
        verticaProp.put("password", password);
        verticaProp.put("AutoCommit", "false");
        return DriverManager.getConnection(url,verticaProp);
    }
    
    private static void copyData(String destObject, int outThreshold, String infnum) throws SQLException, UnsupportedEncodingException {
            String oraSQL, verticaSQL;    
            String divider = null;
            Statement oraStmt = oraConnection.createStatement();
            Statement verticaStmt = verticaConnection.createStatement();
            // Символы, которые необходимо "очистить" перез загрузкой
            char CR = 13;
            char LF = 10;
            char DV = '|';
            // Символ для замены "плохих" символов
            char SPACE = 32;
            char SLSH = '/';
            // Удаляем старые данные
            verticaSQL = "DELETE FROM "+destObject;//+" WHERE ID_PRODC = "+infnum;
            // Запускаем SQL и читаем метеданные
            if (verticaStmt.executeUpdate(verticaSQL) > 0) {
                System.out.println("Удалены данные за период из "+destObject);
                System.out.println("INF #" + infnum);
            }
            // Составляем SQL для чтения данных из Oracle
            oraSQL = "SELECT * FROM "+destObject;// WHERE ID_PRODC = "+infnum;
            // Запускаем SQL и читаем метеданные
            ResultSet orset = oraStmt.executeQuery(oraSQL);
            ResultSetMetaData orsmd = orset.getMetaData();
            // Команда загрузки данных
            String copyQuery = "COPY "+destObject+" FROM STDIN DELIMITER '|' DIRECT";
            // Создаем поток для загрузки
            VerticaCopyStream vstream = new VerticaCopyStream((VerticaConnection) verticaConnection, copyQuery);
            // Стартуем поток для загрузки.
            long results = 0;
            int totalRejects = 0;
            vstream.start();
            // Создаем входной поток.
            String oraData, fieldValue;
            rowCount = 0;
            System.out.println("Загрузка данных.");
            System.out.println("INF #" + infnum);
            while( orset.next() )
                {   // Подготавливаем строку для записи в поток
                    oraData = "";
                    for (int j = 1; j <= orsmd.getColumnCount(); j++)  {
                         if (j == 1) {
                             divider = "";
                         } else {
                             divider = "" + DV;
                         }
                        if (orset.getObject(j) == null) {
                            fieldValue = "";
                        } else {
                            fieldValue = orset.getNString(j).replace(CR, SPACE).replace(LF, SPACE).replace(DV, SLSH);
                        }
                        oraData = oraData + divider + fieldValue;
                    }
                    oraData = oraData + "\n";
                    //System.out.println(oraData);
                    // Связываем потоки 
                    vstream.addStream(new ByteArrayInputStream(oraData.getBytes("UTF-8")));                  
                    rowCount++;
                    if (rowCount % outThreshold == 0) {
                        // Записываем поток в базу
                        results += vstream.finish();
                        System.out.print(new Date());
                        System.out.println(" Обработано строк: "+rowCount);
                        // Показываем статистику загрузки.
                        List<Long> rejects = vstream.getRejects();              
                        // Список отброшенных записей.
                        Iterator<Long> rejit = rejects.iterator();
                        long linecount = 0;
                        while (rejit.hasNext()) {
                            System.out.print("Отброшена #" + ++linecount);
                            System.out.println(" строка " + rejit.next());
                        }
                        // Количество отброшенных записей.
                        totalRejects += rejects.size();
                        // Открываем поток для новой порции данных
                        vstream.start();
                    }
                }
            // Показываем статистику загрузки.
            List<Long> rejects = vstream.getRejects();              
            // Список отброшенных записей.
            Iterator<Long> rejit = rejects.iterator();
            long linecount = 0;
            while (rejit.hasNext()) {
                   System.out.print("Отброшена #" + ++linecount);
                   System.out.println(" строка " + rejit.next());
            }
            // Количество отброшенных записей.
            totalRejects += rejects.size();
            // Закрываем загрузку.
            results += vstream.finish();          
            verticaConnection.commit();
            // Выводим статистику на экран.
            System.out.println("Всего загружено  : " + results);
            System.out.println("Всего отброшено  : " + totalRejects);       
            // Считываем количество загруженных записей из теблицы
            int cnt; // Количество записей 
            verticaSQL = "SELECT COUNT(1) FROM "+destObject;
            // Выполнение запроса
            ResultSet rset = verticaStmt.executeQuery(verticaSQL);
            rset.next();
            cnt = rset.getInt(1);
            System.out.println("Всего в таблице "+destObject+" записей: "+ cnt);      
    }

    public static void main(String[] args) throws IOException  {
        // Объект для копирования данных
        String objName = "FT_CARD_SNAPSHOT";
        // Параметры соединения  для Vertica
        String verticaUser = propertyWorker.loadProperty(propertiesFileName, "verticaUser"); 
        String verticaPassword = propertyWorker.loadProperty(propertiesFileName, "verticaPassword");
        String verticaDBName = propertyWorker.loadProperty(propertiesFileName, "verticaDBName");
        String verticaHost = propertyWorker.loadProperty(propertiesFileName, "verticaHost");
        String verticaPort = propertyWorker.loadProperty(propertiesFileName, "verticaPort");
        // Параметры соединения  для Oracle
        String oraUser = propertyWorker.loadProperty(propertiesFileName, "oraUser"); 
        String oraPassword = propertyWorker.loadProperty(propertiesFileName, "oraPassword");
        String oraDBName = propertyWorker.loadProperty(propertiesFileName, "oraDBName");
        String oraHost = propertyWorker.loadProperty(propertiesFileName, "oraHost");
        String oraPort = propertyWorker.loadProperty(propertiesFileName, "oraPort");
        // Размер буфера для считывания (строк)
        int dataThreshold = new Integer(propertyWorker.loadProperty(propertiesFileName, "dataThreshold", "10000"));
        // Основная работа
        System.out.println(new Date());
        System.out.println("Версия: " + Version);
        System.out.println(Copyright);
        int start = new Integer(args[0]);
        int end = new Integer(args[1]);
        
        try {
            // Создаем соединение c СУБД
            verticaConnection = verticaConnect(verticaUser, verticaPassword, verticaHost, verticaPort, verticaDBName);
            System.out.println("Соединено с  СУБД Vertica. Сервер: " + verticaHost);
            oraConnection = oraConnect(oraUser, oraPassword, oraHost, oraPort, oraDBName);
            System.out.println("Соединено с  СУБД Oracle. Сервер: " + oraHost);
            for (int i = start; i <= end; i++) {
                copyData(oraUser+"."+objName, dataThreshold, String.valueOf(i));
            }
            System.out.println(new Date());
            System.out.println("Копирование объекта "+objName+" закончено.");
        } catch (SQLException | UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
            System.out.println("Ошибка при загрузке строки: " + rowCount);
        }
    }
    
}
