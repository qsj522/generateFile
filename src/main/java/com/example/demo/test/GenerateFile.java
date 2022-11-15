package com.example.demo.test;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import cn.hutool.poi.excel.ExcelWriter;
import cn.hutool.setting.dialect.Props;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @Auther: gnr
 * @Date: 2022/4/2 17:16
 * @Description:
 */
public class GenerateFile {


    @Test
    /**
     * 读取excel文件生成表sql及ddm文件
     */
    public void writeTablesql(){

        //读取配置文件
        Props props = new Props("application.properties");

        //用于存储（表名+id）为key,id约束为value
        Map<String,String> tableMap=new HashMap();
        //生成sql文件的目标路径
        String sqlTargetFilePath=props.getProperty("sqlTargetFilePath");
        //excel表模板文件路径
        String excelPath=props.getProperty("excelPath");
        //生成sql文件目录
        GennerateServicve.writeTablesql(excelPath,sqlTargetFilePath,tableMap);
    }


    /**
     * 生成all_sample.py模板文件
     */
    @Test
    public void write_all_sample_file(){
        //读取配置文件
        Props props = new Props("application.properties");

        //用于存储（表名+id）为key,id约束为value
        Map<String,String> tableMap=new HashMap();
        //生成sql文件的目标路径
        String sqlTargetFilePath=props.getProperty("sqlTargetFilePath");
        //excel表模板文件路径
        String excelPath=props.getProperty("excelPath");
        //数据库名称
        String datebaseName=props.getProperty("datebaseName");
        //获取表sql目标路径下的表名称
        List<String> ftableNameList = Arrays.asList(FileUtil.file(sqlTargetFilePath).list());

        String fileName="all_sample.py";
        //获取工程目录resource/static/pyFileTemplate文件下的python文件模板
        String pyTemplate = FileUtil.readString("static/pyFileTemplate"+File.separator+fileName, Charset.forName("UTF-8"));
        //根据py模板文件的文件名作为生成py文件的后缀名
        String pyFileSuffix=fileName.substring(0,fileName.indexOf("."));
        GennerateServicve.getExcelTableMap(excelPath,sqlTargetFilePath,tableMap);
        GennerateServicve.generateFileMethod(pyTemplate,ftableNameList,pyFileSuffix+".py",sqlTargetFilePath,new File(sqlTargetFilePath).getParent()+File.separator+pyFileSuffix,datebaseName,tableMap);

    }

    /**
     * 生成update_all_clean_fix.py模板文件
     */
    @Test
    public void write_update_all_clean_fix_file(){
        //读取配置文件
        Props props = new Props("application.properties");

        //用于存储（表名+id）为key,id约束为value
        Map<String,String> tableMap=new HashMap();
        //生成sql文件的目标路径
        String sqlTargetFilePath=props.getProperty("sqlTargetFilePath");
        //excel表模板文件路径
        String excelPath=props.getProperty("excelPath");
        //数据库名称
        String datebaseName=props.getProperty("datebaseName");
        //获取表sql目标路径下的表名称
        List<String> ftableNameList = Arrays.asList(FileUtil.file(sqlTargetFilePath).list());

        String fileName="update_all_clean_fix.py";
        //获取工程目录resource/static/pyFileTemplate文件下的python文件模板
        String pyTemplate = FileUtil.readString("static/pyFileTemplate"+File.separator+fileName, Charset.forName("UTF-8"));
        //根据py模板文件的文件名作为生成py文件的后缀名
        String pyFileSuffix=fileName.substring(0,fileName.indexOf("."));
        GennerateServicve.getExcelTableMap(excelPath,sqlTargetFilePath,tableMap);
        GennerateServicve.generateFileMethod(pyTemplate,ftableNameList,pyFileSuffix+".py",sqlTargetFilePath,new File(sqlTargetFilePath).getParent()+File.separator+pyFileSuffix,datebaseName,tableMap);

    }


    /**
     * 生成update_all_clean_fix.py模板文件
     */
    @Test
    public void write_update_all_fix_file(){
        //读取配置文件
        Props props = new Props("application.properties");

        //用于存储（表名+id）为key,id约束为value
        Map<String,String> tableMap=new HashMap();
        //生成sql文件的目标路径
        String sqlTargetFilePath=props.getProperty("sqlTargetFilePath");
        //excel表模板文件路径
        String excelPath=props.getProperty("excelPath");
        //数据库名称
        String datebaseName=props.getProperty("datebaseName");
        //获取表sql目标路径下的表名称
        List<String> ftableNameList = Arrays.asList(FileUtil.file(sqlTargetFilePath).list());

        String fileName="update_all_fix.py";
        //获取工程目录resource/static/pyFileTemplate文件下的python文件模板
        String pyTemplate = FileUtil.readString("static/pyFileTemplate"+File.separator+fileName, Charset.forName("UTF-8"));
        //根据py模板文件的文件名作为生成py文件的后缀名
        String pyFileSuffix=fileName.substring(0,fileName.indexOf("."));
        GennerateServicve.getExcelTableMap(excelPath,sqlTargetFilePath,tableMap);
        GennerateServicve.generateFileMethod(pyTemplate,ftableNameList,pyFileSuffix+".py",sqlTargetFilePath,new File(sqlTargetFilePath).getParent()+File.separator+pyFileSuffix,datebaseName,tableMap);

    }


    /**
     * 生成update_ff_clean_fix.py模板文件
     */
    @Test
    public void write_update_ff_clean_fix_file(){
        //读取配置文件
        Props props = new Props("application.properties");

        //用于存储（表名+id）为key,id约束为value
        Map<String,String> tableMap=new HashMap();
        //生成sql文件的目标路径
        String sqlTargetFilePath=props.getProperty("sqlTargetFilePath");
        //excel表模板文件路径
        String excelPath=props.getProperty("excelPath");
        //数据库名称
        String datebaseName=props.getProperty("datebaseName");
        //获取表sql目标路径下的表名称
        List<String> ftableNameList = Arrays.asList(FileUtil.file(sqlTargetFilePath).list());

        String fileName="update_ff_clean_fix.py";
        //获取工程目录resource/static/pyFileTemplate文件下的python文件模板
        String pyTemplate = FileUtil.readString("static/pyFileTemplate"+File.separator+fileName, Charset.forName("UTF-8"));
        //根据py模板文件的文件名作为生成py文件的后缀名
        String pyFileSuffix=fileName.substring(0,fileName.indexOf("."));
        GennerateServicve.getExcelTableMap(excelPath,sqlTargetFilePath,tableMap);
        GennerateServicve.generateFileMethod(pyTemplate,ftableNameList,pyFileSuffix+".py",sqlTargetFilePath,new File(sqlTargetFilePath).getParent()+File.separator+pyFileSuffix,datebaseName,tableMap);

    }


    /**
     * 生成update_sample.py模板文件
     */
    @Test
    public void write_update_sample_file(){
        //读取配置文件
        Props props = new Props("application.properties");

        //用于存储（表名+id）为key,id约束为value
        Map<String,String> tableMap=new HashMap();
        //生成sql文件的目标路径
        String sqlTargetFilePath=props.getProperty("sqlTargetFilePath");
        //excel表模板文件路径
        String excelPath=props.getProperty("excelPath");
        //数据库名称
        String datebaseName=props.getProperty("datebaseName");
        //获取表sql目标路径下的表名称
        List<String> ftableNameList = Arrays.asList(FileUtil.file(sqlTargetFilePath).list());

        String fileName="update_sample.py";
        //获取工程目录resource/static/pyFileTemplate文件下的python文件模板
        String pyTemplate = FileUtil.readString("static/pyFileTemplate"+File.separator+fileName, Charset.forName("UTF-8"));
        //根据py模板文件的文件名作为生成py文件的后缀名
        String pyFileSuffix=fileName.substring(0,fileName.indexOf("."));
        GennerateServicve.getExcelTableMap(excelPath,sqlTargetFilePath,tableMap);
        GennerateServicve.generateFileMethod(pyTemplate,ftableNameList,pyFileSuffix+".py",sqlTargetFilePath,new File(sqlTargetFilePath).getParent()+File.separator+pyFileSuffix,datebaseName,tableMap);

    }



    /**
     * 读取sql文件中的hive建表语句
     */
    public String redHIveSql(String sqlPath){
        //sql文件路径
        String sqlFileContent = FileUtil.readString(FileUtil.file(sqlPath), Charset.forName("UTF-8"));
        //截取HIVE创建表语句
        String creatHiveSql = sqlFileContent.substring(sqlFileContent.indexOf("HIVE建表：")+7, sqlFileContent.indexOf("spark建立ff表")).trim();
        return creatHiveSql;

    }

    /**
     * 读取sql文件中的spark建立ff表语句
     */

    public String redSpark_ff_Sql(String sqlPath){
        //sql文件路径
        String sqlFileContent = FileUtil.readString(FileUtil.file(sqlPath), Charset.forName("UTF-8"));
        //截取HIVE创建表语句
        String creatSparkffSql = sqlFileContent.substring(sqlFileContent.indexOf("spark建立ff表：")+11, sqlFileContent.indexOf("spark建立m表：")).trim();
        return creatSparkffSql;


    }

    /**
     * 读取sql文件中的spark建立ff表语句
     */

    public String redSpark_m_Sql(String sqlPath){
        //sql文件路径
        String sqlFileContent = FileUtil.readString(FileUtil.file(sqlPath), Charset.forName("UTF-8"));
        //截取HIVE创建表语句
        String creatSpark_m_Sql = sqlFileContent.substring(sqlFileContent.indexOf("spark建立m表：")+10, sqlFileContent.indexOf("back方案：")).trim();
        return creatSpark_m_Sql;


    }

}
