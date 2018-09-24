/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package OperLoader;

import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

/**
 *
 * @author VErin
 */
public class propertyWorker { 
    private static final String sDirSeparator = System.getProperty("file.separator");
    private static final Properties props = new Properties();
    private static final File currentDir = new File(".");
    // ������ �������� �� �����
    public static String loadProperty(String propFileName, String propName) throws FileNotFoundException, IOException {
        // ���������� ������ ���� � �����
        String sFilePath = currentDir.getCanonicalPath() + sDirSeparator + propFileName;
        FileInputStream ins = new FileInputStream(sFilePath);
        props.load(ins);
        // ������� �������� ��� ��������
        return props.getProperty(propName).trim();
    }
    
    public static String loadProperty(String propFileName, String propName, String defValue ) throws FileNotFoundException, IOException {
        // ���������� ������ ���� � �����
        String sFilePath = currentDir.getCanonicalPath() + sDirSeparator + propFileName;
        FileInputStream ins = new FileInputStream(sFilePath);
        props.load(ins);
        // ������� �������� ��� ��������
        return props.getProperty(propName, defValue).trim();
    }
    // ���������� �������� � ����
    public static void saveProperty(String propFileName, String propName, String propValue) throws FileNotFoundException, IOException {
        // ���������� ������ ���� � �����
        String sFilePath = currentDir.getCanonicalPath() + sDirSeparator + propFileName;
        FileOutputStream outs = new FileOutputStream(sFilePath);
        // ������������� �������� ��������
        props.setProperty(propName, propValue);
        // ���������� � ����� �������� ��� ��������
        props.store(outs, null);
   }
}
