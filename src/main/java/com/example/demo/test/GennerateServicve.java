package com.example.demo.test;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import cn.hutool.poi.excel.ExcelWriter;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Auther: g27937
 * @Date: 2022/4/18 17:43
 * @Description:
 */
public class GennerateServicve {

    //生成py文件方法

    /**
     *
     * @param pyfile py文件模板内容
     * @param ftableNameList f表名称，多个表名称用英文逗号隔开
     * @param pyFilesuffix py文件后缀名称
     * @param ftableNamePath f表父级目录
     * @param pyFilePath 生成py文件目录
     * @param datebaseName 数据表名称
     */
    public static void generateFileMethod(String pyfile, List<String> ftableNameList , String pyFilesuffix, String ftableNamePath, String pyFilePath, String datebaseName, Map<String,String> tableMap) {


        for (int i = 0; i < ftableNameList.size(); i++) {
            String temp = pyfile;
            String ftableName = ftableNameList.get(i);
            String fftableName = ftableName.replace("_f_", "_ff_");
            String mtableName = ftableName.replace("_f_", "_m_");

            //读取字段
            String s = FileUtil.readString(FileUtil.file(ftableNamePath + File.separator + ftableName + File.separator + ftableName + ".sql"), Charset.forName("UTF-8"));
            String ziduan = s.substring(s.indexOf("--原表字段") + 6, s.length()).trim();
            StringBuffer mziduanBuffer = new StringBuffer();
            StringBuffer aziduanBuffer = new StringBuffer();
            String[] split = ziduan.split(",");
            for (String s1 : split) {
                mziduanBuffer.append("m.").append(s1.trim()).append(",").append("\n");
                aziduanBuffer.append("a.").append(s1.trim()).append(",").append("\n");
            }
            String pyFilename = ftableName + pyFilesuffix;

            temp = temp.replaceAll("ff的表名称", fftableName);
            temp = temp.replaceAll("f表名称", ftableName);
            temp = temp.replaceAll("m表名称", mtableName);
            temp = temp.replaceAll("你的字段,", ziduan);
            temp = temp.replaceAll("你的字段", ziduan);
            temp = temp.replaceAll("数据库", datebaseName);
            temp = temp.replaceAll("m\\.的字段", mziduanBuffer.toString());
            temp = temp.replaceAll("a\\.的字段", aziduanBuffer.toString());
            temp = temp.replaceAll("'主键Id1-n'", tableMap.get(ftableName+"ids"));
            temp = temp.replaceAll("主键", tableMap.get(ftableName+"id"));
            temp = temp.replace(".filter()",tableMap.get(ftableName+"filter"));

            //写入文件
            FileUtil.writeString(temp, new File(pyFilePath + File.separator + pyFilename), Charset.forName("UTF-8"));
        }



    }

    //生成sql文件

    /**
     *
     * @param excelPath excel文档路径
     * @param targetFilePath 生成sql文件的目标路径
     */
    public static void writeTablesql(String excelPath,String targetFilePath,Map<String,String> tableMap){

        String content="命名格式：\n" +
                "{HDFS Base Path}/dl_{简写1)}/{主题简写}_{数据分层缩写2)}_{数据简述}/{数据名}.{文件扩展名}\n" +
                "\n" +
                "hdfs路径： \n" +
                "/apps/hive/warehouse/external/dl_scdp/scdp_f_表名称/scdp_f_表名称\n" +
                "\n" +
                "\n" +
                "HIVE建表：\n" +
                "\n" +
                "\n" +
                "spark建立ff表：\n" +
                "\n" +
                "\n" +
                "\n" +
                "spark建立m表：\n" +
                "\n" +
                "\n" +
                "back方案：\n" +
                "drop table dl_scdp.scdp_f_表名称;\n" +
                "drop table dl_scdp.scdp_ff_表名称;\n" +
                "drop table dl_scdp.scdp_m_表名称;\n" +
                "\n" +
                "--原表字段";

        String f="create external table dl_scdp.scdp_f_表名称 (\n" +
                "\t 字段 \n" +
                "        ) \n" +
                "ROW FORMAT DELIMITED fields terminated by '\\\\t'\n" +
                "STORED AS TEXTFILE\n" +
                "location '/apps/hive/warehouse/external/dl_scdp/scdp_f_表名称';";
        String ff="create table dl_scdp.scdp_ff_表名称 (\n" +
                "  字段 \n" +
                " dl_loaded_dt timestamp\n" +
                ") PARTITIONED BY (day int) STORED AS parquet;";

        String m="create table dl_scdp.scdp_m_表名称 (\n" +
                " dl_id varchar(50),\n" +
                " 字段 \n" +
                " dl_loaded_dt timestamp,\n" +
                " dl_start_dt timestamp,\n" +
                " dl_end_dt timestamp\n" +
                ") PARTITIONED BY (dl_status varchar(10),day int) STORED AS parquet";

        String tableName="erp_apps.h3c_isp_cmp_wipc_for_scdp_v";
//        String tableNameprefix="scdp_f_";


        List<String> fileNameList = FileUtil.listFileNames(excelPath);
        //用于存储（表名+id）为key,id约束为value

        for (String fileName : fileNameList) {
            tableName=fileName.substring(0,fileName.lastIndexOf(".")).replaceAll("\\.","_").replaceAll("-","_").toLowerCase();
            ExcelReader reader = ExcelUtil.getReader(FileUtil.file(excelPath+File.separator+fileName));
            ExcelReader readerTable = ExcelUtil.getReader(excelPath+File.separator+fileName,1);
            List<Map<String, Object>> readAll = reader.readAll();

            String[] tableNameprefix = {"scdp_f_"+tableName,"scdp_ff_"+tableName,"scdp_m_"+tableName};
            //生成ddm文件
            GnerrateDDMExcelFile(readerTable.readAll(), reader.readAll(),tableNameprefix,targetFilePath);

            //建表语句 字段加类型
            StringBuffer fieldAndtypeBuffer=new StringBuffer();
            //建表语句 原表字段
            StringBuffer fieldBuffer=new StringBuffer();

            StringBuffer col_filter=new StringBuffer();//字段约束
            StringBuffer id=new StringBuffer();
            StringBuffer ids=new StringBuffer();

            try {
                readAll.stream().forEach(e->{


                    if(e.get("col_name")==null){
                        return;
                    }
                    String col_name = e.get("col_name").toString().toLowerCase();
                    String col_type = e.get("col_type").toString().toLowerCase();
                    //字段转义
                    if("number".equals(col_type)){
                        col_type="int";
                    }else if(col_type.indexOf("number(")!=-1){
                        Integer integer = Integer.valueOf(col_type.substring(col_type.indexOf("number(") + 7, col_type.length() - 1));
                        if(integer>10){
                            col_type="bigint";
                        }
                        if(integer<=10){
                            col_type="int";
                        }
                    }else if(col_type.indexOf("varchar2")!=-1){
                        col_type=col_type.replaceAll("varchar2","varchar");
                    }
                    else if(col_type.indexOf("date")!=-1){
                        col_type="timestamp";
                    }
                    else if(col_type.indexOf("clob")!=-1){
                        col_type=col_type.replaceAll("clob","varchar(5000)");
                    }
                    else if(col_type.indexOf("integer")!=-1){
                        col_type=col_type.replaceAll("integer","double");
                    }
                    if(ReUtil.contains("[varchar\\(][0-9][\\)]",col_type)){
                        col_type="varchar(10)";
                    }
                    if(col_type.indexOf("nvarchar")!=-1){
                        col_type=col_type.replaceAll("nvarchar","varchar");
                    }

                    if(col_type.indexOf("rowid")!=-1){
                        col_type=col_type.replaceAll("rowid","varchar(10)");
                    }

                    if(col_type.indexOf("narchar")!=-1){
                        col_type=col_type.replaceAll("narchar","varchar");
                    }

                    String col_constraint = e.get("col_constraint").toString().toLowerCase();
                    if(col_constraint.indexOf("主键")!=-1){
                        id.append("m.").append(col_name).append("=").append("n.").append(col_name).append(" and ");
                        ids.append("'").append(col_name).append("'").append(",");
                        col_filter.append(".filter(\""+col_name+" is not null\") \\\n");
                    }
                    if(col_constraint.indexOf("非空")!=-1){
                        col_filter.append(".filter(\""+col_name+" is not null\") \\\n");
                    }

                    String col_desc = e.get("col_desc").toString();

                    fieldAndtypeBuffer.append(col_name).append(" ").append(col_type).append(",").append("\n");
                    fieldBuffer.append(col_name).append(",").append("\n");
                });
            } catch (Exception e) {

                e.printStackTrace();
                System.out.println(fileName+"模板excle文件格式可能有误");
                break;
            }
            if(ids==null||id.toString().equals("")){
                System.out.println(fileName+"文件没有主键,将影响py文件生成");
            }
            tableMap.put(tableNameprefix[0]+"ids",  ids.toString().trim().replaceAll(",$",""));
            tableMap.put(tableNameprefix[0]+"id",  id.toString().trim().replaceAll("and$",""));
            tableMap.put(tableNameprefix[0]+"filter",  col_filter.toString());
            tableMap.put(tableNameprefix[0]+"fieldAndtype",fieldAndtypeBuffer.toString());
            tableMap.put(tableNameprefix[0]+"field",fieldBuffer.toString());

            String fTemp=f;
            String ffTemp=ff;
            String mTemp=m;
            fTemp= SQLUtils.format(fTemp.replaceAll("字段",fieldAndtypeBuffer.toString()), JdbcConstants.HIVE);
            ffTemp=SQLUtils.format(ffTemp.replaceAll("字段",fieldAndtypeBuffer.toString()), JdbcConstants.HIVE);
            mTemp=SQLUtils.format(mTemp.replaceAll("字段",fieldAndtypeBuffer.toString()), JdbcConstants.HIVE);
            //替换表名称
            String contentTemp=content;
            contentTemp=contentTemp.replaceAll("HIVE建表：","HIVE建表："+"\n"+fTemp);
            contentTemp=contentTemp.replaceAll("spark建立ff表：","spark建立ff表："+"\n"+ffTemp);
            contentTemp=contentTemp.replaceAll("spark建立m表：","spark建立m表："+"\n"+mTemp);
            contentTemp=contentTemp.replaceAll("表名称",tableName);
            contentTemp=contentTemp+"\n"+fieldBuffer.toString();
            //写入文件
            FileUtil.writeString(contentTemp,new File(targetFilePath+File.separator+File.separator+tableNameprefix[0]+File.separator+tableNameprefix[0]+".sql"),Charset.forName("UTF-8"));
        }

    }











    /**
     *获取Excel中对应的表信息
     * @param excelPath excel文档路径
     * @param targetFilePath 生成sql文件的目标路径
     */
    public static void getExcelTableMap(String excelPath,String targetFilePath,Map<String,String> tableMap){


        String tableName="";
//        String tableNameprefix="scdp_f_";

        List<String> fileNameList = FileUtil.listFileNames(excelPath);
        //用于存储（表名+id）为key,id约束为value

        for (String fileName : fileNameList) {
            tableName=fileName.substring(0,fileName.lastIndexOf(".")).replaceAll("\\.","_").replaceAll("-","_").toLowerCase();
            ExcelReader reader = ExcelUtil.getReader(FileUtil.file(excelPath+File.separator+fileName));
            List<Map<String, Object>> readAll = reader.readAll();
            ExcelReader readerTable = ExcelUtil.getReader(excelPath+File.separator+fileName,1);

            String[] tableNameprefix = {"scdp_f_"+tableName,"scdp_ff_"+tableName,"scdp_m_"+tableName};
            //生成ddm文件
            GnerrateDDMExcelFile( readerTable.readAll(), reader.readAll(),tableNameprefix,targetFilePath);

            //建表语句 字段加类型
            StringBuffer fieldAndtypeBuffer=new StringBuffer();
            //建表语句 原表字段
            StringBuffer fieldBuffer=new StringBuffer();

            StringBuffer col_filter=new StringBuffer();//字段约束
            StringBuffer id=new StringBuffer();
            StringBuffer ids=new StringBuffer();

            try {
                readAll.stream().forEach(e->{


                    if(e.get("col_name")==null){
                        return;
                    }
                    String col_name = e.get("col_name").toString().toLowerCase();
                    String col_type = e.get("col_type").toString().toLowerCase();
                    //字段转义
                    if("number".equals(col_type)){
                        col_type="int";
                    }else if(col_type.indexOf("number(")!=-1){
                        Integer integer = Integer.valueOf(col_type.substring(col_type.indexOf("number(") + 7, col_type.length() - 1));
                        if(integer>10){
                            col_type="bigint";
                        }
                        if(integer<=10){
                            col_type="int";
                        }
                    }else if(col_type.indexOf("varchar2")!=-1){
                        col_type=col_type.replaceAll("varchar2","varchar");
                    }
                    else if(col_type.indexOf("date")!=-1){
                        col_type="timestamp";
                    }
                    else if(col_type.indexOf("clob")!=-1){
                        col_type=col_type.replaceAll("clob","varchar(5000)");
                    }
                    else if(col_type.indexOf("integer")!=-1){
                        col_type=col_type.replaceAll("integer","double");
                    }
                    if(ReUtil.contains("[varchar\\(][0-9][\\)]",col_type)){
                        col_type="varchar(10)";
                    }
                    if(col_type.indexOf("nvarchar")!=-1){
                        col_type=col_type.replaceAll("nvarchar","varchar");
                    }

                    if(col_type.indexOf("rowid")!=-1){
                        col_type=col_type.replaceAll("rowid","varchar(10)");
                    }

                    if(col_type.indexOf("narchar")!=-1){
                        col_type=col_type.replaceAll("narchar","varchar");
                    }

                    String col_constraint = e.get("col_constraint").toString().toLowerCase();
                    if(col_constraint.indexOf("主键")!=-1){
                        id.append("m.").append(col_name).append("=").append("n.").append(col_name).append(" and ");
                        ids.append("'").append(col_name).append("'").append(",");
                        col_filter.append(".filter(\""+col_name+" is not null\") \\\n");
                    }
                    if(col_constraint.indexOf("非空")!=-1){
                        col_filter.append(".filter(\""+col_name+" is not null\") \\\n");
                    }

                    String col_desc = e.get("col_desc").toString();

                    fieldAndtypeBuffer.append(col_name).append(" ").append(col_type).append(",").append("\n");
                    fieldBuffer.append(col_name).append(",").append("\n");
                });
            } catch (Exception e) {

                e.printStackTrace();
                System.out.println(fileName+"模板excle文件格式可能有误");
                break;
            }
            if(ids==null||id.toString().equals("")){
                System.out.println(fileName+"文件没有主键,将影响py文件生成");
            }
            tableMap.put(tableNameprefix[0]+"ids",  ids.toString().trim().replaceAll(",$",""));
            tableMap.put(tableNameprefix[0]+"id",  id.toString().trim().replaceAll("and$",""));
            tableMap.put(tableNameprefix[0]+"filter",  col_filter.toString());
            tableMap.put(tableNameprefix[0]+"fieldAndtype",fieldAndtypeBuffer.toString());
            tableMap.put(tableNameprefix[0]+"field",fieldBuffer.toString());
        }

    }









    /**
     *
     * @param rows 读取入湖表excle模板的rows信息
     * @param tableNames 入湖表名称
     */
    public static void GnerrateDDMExcelFile( List<Map<String, Object>> tableRows , List<Map<String, Object>> rows,String[] tableNames,String targetSqlPath){

        List<Map<String, Object>> rowsTemp=new ArrayList<>();
        for (String tableName : tableNames)
        {
            rowsTemp.clear();
            rowsTemp.addAll(new ArrayList<>(rows));
            rowsTemp.remove(rowsTemp.size() -1);
            rowsTemp.stream().forEach(e->{
                e.put("data_source","Hive");
                e.put("schema_name","dl_scdp");
                e.put("sec_lvl_id",StrUtil.isBlank(e.get("sec_lvl_id").toString())?"":e.get("sec_lvl_id").toString().trim());
                e.put("tbl_name",tableName);
                e.put("col_name",e.get("col_name").toString().trim().toLowerCase());
                e.put("col_desc",StrUtil.isBlank(e.get("col_desc").toString())?"":e.get("col_desc").toString().trim());
                e.put("comments",StrUtil.isBlank(e.get("comments").toString())?"":e.get("comments").toString().trim());
                e.put("col_constraint",StrUtil.isBlank(e.get("col_constraint").toString())?"":(e.get("col_constraint").toString().contains("[")?e.get("col_constraint").toString():"['"+e.get("col_constraint").toString()+"']"));
                e.put("sample_data",StrUtil.isBlank(e.get("sample_data").toString())?"":e.get("sample_data").toString().trim());
                e.remove("col_type");
            });

            //复制excle表模板
            File file = FileUtil.file("static/excelTemplate/sample.xlsx");

            String ddmPath=targetSqlPath+File.separator+File.separator+tableNames[0]+File.separator+ File.separator + tableName+"_ddm.xlsx";
            FileUtil.copy(file, FileUtil.file(ddmPath),true);

            //通过工具类创建writer
            ExcelWriter writer = ExcelUtil.getWriter(ddmPath);

            writer.setSheet("column");
            Workbook workbook = writer.getWorkbook();
            //删除由于新增的sheet
            workbook.removeSheetAt(2);

            writer.write(rowsTemp);

            tableRows.stream().forEach(e->{
                e.put("schema_name","dl_scdp");
                e.put("tbl_name",tableName);
                e.put("data_category",e.get("data_category"));
                e.put("themes",e.get("themes"));
                e.put("owner_sys",e.get("owner_sys"));
                e.put("owner",e.get("owner"));
                e.put("tbl_desc",e.get("tbl_desc"));
                e.put("data_frequency",e.get("data_frequency"));
                e.put("sec_lvl_id",e.get("sec_lvl_id"));
                e.put("comments",e.get("comments"));
                e.put("data_source",e.get("data_source"));
                e.put("tbl_std_lvl",e.get("tbl_std_lvl"));
            });

            writer.setSheet("table");
            writer.write(tableRows);
            //关闭writer，释放内存
            writer.close();

            System.out.println(tableName);
        }
        //处理内容


    }
}
