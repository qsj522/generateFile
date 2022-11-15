package com.example.demo.test;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;

/**
 * @Auther: gnr
 * @Date: 2022/4/2 17:16
 * @Description:
 */
public class GenerateFileAll {

    public static void main(String[] args) {

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


        //数据库名称
        String datebaseName=props.getProperty("datebaseName");
        //获取表sql目标路径下的表名称
        List<String> ftableNameList = Arrays.asList(FileUtil.file(sqlTargetFilePath).list());

        File file=FileUtil.file("static/pyFileTemplate");
        String[] list = file.list();
        Arrays.stream(list).forEach(fileName->{
            //获取工程目录resource/static/pyFileTemplate文件下的python文件模板
            String pyTemplate = FileUtil.readString(file.getPath()+File.separator+fileName, Charset.forName("UTF-8"));
            //根据py模板文件的文件名作为生成py文件的后缀名
            String pyFileSuffix=fileName.substring(0,fileName.indexOf("."));
            GennerateServicve.generateFileMethod(pyTemplate,ftableNameList,pyFileSuffix+".py",sqlTargetFilePath,new File(sqlTargetFilePath).getParent()+File.separator+pyFileSuffix,datebaseName,tableMap);

        });
    }










}
