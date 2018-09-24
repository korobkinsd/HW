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
    private static Connection oraConnection;     // ���������� ���������� Oracle
    private static Connection verticaConnection; // ���������� ���������� Vertica
    private static int rowCount;
    private static final String Version = "1.1.00";
    private static final String propertiesFileName = "loader.properties"; 
    private static final String Copyright = "����������� \"����� ����\", 2016"; 
 
    private static Connection oraConnect(String username, String password, String server, String port, String sid) throws SQLException {
        String url;
        // ������ ���������� ��� JDBC �������� Oracle (thin)
        url = "jdbc:oracle:thin:@" + server + ":" + port + "/" + sid;
        return DriverManager.getConnection(url, username, password);
    }
    
    private static Connection verticaConnect(String username, String password, String server, String port, String dbName) throws SQLException {
        String url;
        // ������ ���������� ��� JDBC �������� Vertica
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
            // �������, ������� ���������� "��������" ����� ���������
            char CR = 13;
            char LF = 10;
            char DV = '|';
            // ������ ��� ������ "������" ��������
            char SPACE = 32;
            char SLSH = '/';
            // ������� ������ ������
            verticaSQL = "DELETE FROM "+destObject;//+" WHERE ID_PRODC = "+infnum;
            // ��������� SQL � ������ ����������
            if (verticaStmt.executeUpdate(verticaSQL) > 0) {
                System.out.println("������� ������ �� ������ �� "+destObject);
                System.out.println("INF #" + infnum);
            }
            // ���������� SQL ��� ������ ������ �� Oracle
            oraSQL = "SELECT * FROM "+destObject;// WHERE ID_PRODC = "+infnum;
            // ��������� SQL � ������ ����������
            ResultSet orset = oraStmt.executeQuery(oraSQL);
            ResultSetMetaData orsmd = orset.getMetaData();
            // ������� �������� ������
            String copyQuery = "COPY "+destObject+" FROM STDIN DELIMITER '|' DIRECT";
            // ������� ����� ��� ��������
            VerticaCopyStream vstream = new VerticaCopyStream((VerticaConnection) verticaConnection, copyQuery);
            // �������� ����� ��� ��������.
            long results = 0;
            int totalRejects = 0;
            vstream.start();
            // ������� ������� �����.
            String oraData, fieldValue;
            rowCount = 0;
            System.out.println("�������� ������.");
            System.out.println("INF #" + infnum);
            while( orset.next() )
                {   // �������������� ������ ��� ������ � �����
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
                    // ��������� ������ 
                    vstream.addStream(new ByteArrayInputStream(oraData.getBytes("UTF-8")));                  
                    rowCount++;
                    if (rowCount % outThreshold == 0) {
                        // ���������� ����� � ����
                        results += vstream.finish();
                        System.out.print(new Date());
                        System.out.println(" ���������� �����: "+rowCount);
                        // ���������� ���������� ��������.
                        List<Long> rejects = vstream.getRejects();              
                        // ������ ����������� �������.
                        Iterator<Long> rejit = rejects.iterator();
                        long linecount = 0;
                        while (rejit.hasNext()) {
                            System.out.print("��������� #" + ++linecount);
                            System.out.println(" ������ " + rejit.next());
                        }
                        // ���������� ����������� �������.
                        totalRejects += rejects.size();
                        // ��������� ����� ��� ����� ������ ������
                        vstream.start();
                    }
                }
            // ���������� ���������� ��������.
            List<Long> rejects = vstream.getRejects();              
            // ������ ����������� �������.
            Iterator<Long> rejit = rejects.iterator();
            long linecount = 0;
            while (rejit.hasNext()) {
                   System.out.print("��������� #" + ++linecount);
                   System.out.println(" ������ " + rejit.next());
            }
            // ���������� ����������� �������.
            totalRejects += rejects.size();
            // ��������� ��������.
            results += vstream.finish();          
            verticaConnection.commit();
            // ������� ���������� �� �����.
            System.out.println("����� ���������  : " + results);
            System.out.println("����� ���������  : " + totalRejects);       
            // ��������� ���������� ����������� ������� �� �������
            int cnt; // ���������� ������� 
            verticaSQL = "SELECT COUNT(1) FROM "+destObject;
            // ���������� �������
            ResultSet rset = verticaStmt.executeQuery(verticaSQL);
            rset.next();
            cnt = rset.getInt(1);
            System.out.println("����� � ������� "+destObject+" �������: "+ cnt);      
    }

    public static void main(String[] args) throws IOException  {
        // ������ ��� ����������� ������
        String objName = "FT_CARD_SNAPSHOT";
        // ��������� ����������  ��� Vertica
        String verticaUser = propertyWorker.loadProperty(propertiesFileName, "verticaUser"); 
        String verticaPassword = propertyWorker.loadProperty(propertiesFileName, "verticaPassword");
        String verticaDBName = propertyWorker.loadProperty(propertiesFileName, "verticaDBName");
        String verticaHost = propertyWorker.loadProperty(propertiesFileName, "verticaHost");
        String verticaPort = propertyWorker.loadProperty(propertiesFileName, "verticaPort");
        // ��������� ����������  ��� Oracle
        String oraUser = propertyWorker.loadProperty(propertiesFileName, "oraUser"); 
        String oraPassword = propertyWorker.loadProperty(propertiesFileName, "oraPassword");
        String oraDBName = propertyWorker.loadProperty(propertiesFileName, "oraDBName");
        String oraHost = propertyWorker.loadProperty(propertiesFileName, "oraHost");
        String oraPort = propertyWorker.loadProperty(propertiesFileName, "oraPort");
        // ������ ������ ��� ���������� (�����)
        int dataThreshold = new Integer(propertyWorker.loadProperty(propertiesFileName, "dataThreshold", "10000"));
        // �������� ������
        System.out.println(new Date());
        System.out.println("������: " + Version);
        System.out.println(Copyright);
        int start = new Integer(args[0]);
        int end = new Integer(args[1]);
        
        try {
            // ������� ���������� c ����
            verticaConnection = verticaConnect(verticaUser, verticaPassword, verticaHost, verticaPort, verticaDBName);
            System.out.println("��������� �  ���� Vertica. ������: " + verticaHost);
            oraConnection = oraConnect(oraUser, oraPassword, oraHost, oraPort, oraDBName);
            System.out.println("��������� �  ���� Oracle. ������: " + oraHost);
            for (int i = start; i <= end; i++) {
                copyData(oraUser+"."+objName, dataThreshold, String.valueOf(i));
            }
            System.out.println(new Date());
            System.out.println("����������� ������� "+objName+" ���������.");
        } catch (SQLException | UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
            System.out.println("������ ��� �������� ������: " + rowCount);
        }
    }
    
}
