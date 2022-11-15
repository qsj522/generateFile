package com.example.demo.test;

import cn.hutool.core.io.FileUtil;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class Update_ff_clean_fix {

    public static void main(String[] args) {

        //获取工程目录resource/static/pyFileTemplate文件下的py文件模板
        String pyTemplate = FileUtil.readString("C:\\Users\\z26919\\Desktop\\项目记录\\供需一体化\\项目资料\\02方案设计\\入湖准备--请勿动\\code\\generateFile\\src\\main\\resources\\static\\pyFileTemplate\\update_ff_clean_fix.py",  Charset.forName("UTF-8"));
        File file = FileUtil.file("C:\\Users\\z26919\\Desktop\\项目记录\\供需一体化\\项目资料\\02方案设计\\入湖准备--请勿动\\基础准备\\增量");
        String[] list = file.list();
        assert list != null;
        List<String> ftableNameList = Arrays.asList(list);
        String pyFilesuffix = "_update_ff_clean_fix.py";
        String ftableNamePath = "C:\\Users\\z26919\\Desktop\\项目记录\\供需一体化\\项目资料\\02方案设计\\入湖准备--请勿动\\基础准备\\增量";
        String pyFilePath = "C:\\Users\\z26919\\Desktop\\update_ff_clean_fix";
        String datebaseName = "dl_scdp";
        generateupdate_ff_clean_fix(pyTemplate, ftableNameList,
                pyFilesuffix, ftableNamePath,
                pyFilePath, datebaseName);
    }


    public static void generateupdate_ff_clean_fix(String pyfile, List<String> ftableNameList,
                                                   String pyFilesuffix, String ftableNamePath,
                                                   String pyFilePath, String datebaseName) {


        for (int i = 0; i < ftableNameList.size(); i++) {
            String temp = pyfile;
            String ftableName = ftableNameList.get(i);
            String fftableName = ftableName.replace("_f_", "_ff_");
            String mtableName = ftableName.replace("_f_", "_m_");

            //读取字段
            String s = FileUtil.readString(FileUtil.file(ftableNamePath + File.separator + ftableName + File.separator + ftableName + ".sql"), Charset.forName("UTF-8"));
            String ziduan = s.substring(s.indexOf("--原表字段") + 6, s.length()).trim();

            String pyFilename = ftableName + pyFilesuffix;

            temp = temp.replaceAll("f表名称", ftableName);
            temp = temp.replaceAll("m表名称", mtableName);
            temp = temp.replaceAll("你的字段,", ziduan);
            temp = temp.replaceAll("你的字段", ziduan);
            temp = temp.replaceAll("ff的表名称", fftableName);
            temp = temp.replaceAll("数据库", datebaseName);

            //写入文件
            FileUtil.writeString(temp, new File(pyFilePath + File.separator + pyFilename), StandardCharsets.UTF_8);
        }
    }
}
