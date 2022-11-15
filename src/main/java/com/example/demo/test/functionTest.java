package com.example.demo.test;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.resource.ClassPathResource;
import cn.hutool.core.util.ReUtil;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 测试工具类
 *
 * @author g27937
 * @date 2022/03/24
 */

public class functionTest {


    /**
     * 根据py模板文件生成py文件
     */
    @Test
    public void generateFile(){
        //获取工程目录resource/static/pyFileTemplate文件下的py文件模板
        String pyTemplate = FileUtil.readString("static/pyFileTemplate/update_sample.py", Charset.forName("UTF-8"));
        File file = FileUtil.file("C:\\Users\\g27937\\Desktop\\入湖\\supply_demand_platform\\02方案设计\\入湖准备--请勿动\\基础准备\\全量");
        String[] list = file.list();
        List<String> ftableNameList = Arrays.asList(list);
        String pyFilesuffix="_update.py";
        String ftableNamePath="C:\\Users\\g27937\\Desktop\\入湖\\supply_demand_platform\\02方案设计\\入湖准备--请勿动\\基础准备";
        String pyFilePath="C:\\Users\\g27937\\Desktop\\saple";
        String datebaseName="dl_scdp";
        generateFileMethod(pyTemplate,ftableNameList,pyFilesuffix,ftableNamePath,pyFilePath,datebaseName);

    }


    /**
     * 根据py模板文件生成py文件
     */
    @Test
    public void generateCleanFile(){
        //获取工程目录resource/static/pyFileTemplate文件下的py文件模板
        String pyTemplate = FileUtil.readString("static/pyFileTemplate/update_all_clean_fix.py", Charset.forName("UTF-8"));
        File file = FileUtil.file("C:\\Users\\g27937\\Desktop\\入湖\\supply_demand_platform\\02方案设计\\入湖准备--请勿动\\基础准备\\全量");
        String[] list = file.list();
        List<String> ftableNameList = Arrays.asList(list);
        //String ftableNames="scdp_f_apps_h3c_wip_for_scdp_v,scdp_f_apps_mrp_item_sourcing_levels_v,scdp_f_erp_apps_h3c_isp_cmp_oppo_for_scdp_v,scdp_f_erp_apps_h3c_isp_cmp_wipc_for_scdp_v,scdp_f_erp_apps_h3c_po_asl_for_scdp_v,scdp_f_fnd_lookup_values_vl,scdp_f_h3c_bom_exploder_bom_data,scdp_f_h3c_it_assembly_materials_mv,scdp_f_hwcust_h3c_vmi_reservations,scdp_f_inv_mtl_material_transactions,scdp_f_inv_mtl_material_transactions_temp,scdp_f_inv_mtl_onhand_quantities_detail,scdp_f_inv_mtl_system_items_b,scdp_f_mrp_gross_requirements,scdp_f_mtl_reservations,scdp_f_mtl_secondary_inventories,scdp_f_mtl_supply,scdp_f_per_all_people_f,scdp_f_pmp_sc_production,scdp_f_pmp_sp_viewreserveplan,scdp_f_pmpquery_h3c_isp_genoppo_new_v,scdp_f_po_vendors,scdp_f_user010,scdp_f_viewfcvmilocksetting,scdp_f_viewiteminflogicmappingforscdp,scdp_f_wip_requirement_operations";
        String pyFilesuffix="_update_all_clean_fix.py";
        String ftableNamePath="C:\\Users\\g27937\\Desktop\\入湖\\supply_demand_platform\\02方案设计\\入湖准备--请勿动\\基础准备";
        String pyFilePath="C:\\Users\\g27937\\Desktop\\pyfile";
        String datebaseName="dl_scdp";
        generateFileMethod(pyTemplate,ftableNameList,pyFilesuffix,ftableNamePath,pyFilePath,datebaseName);

    }




    //生成update_all_clean_fix.py文件
    @Test
    public void redFIle() {
        String pyfile1="import pyspark\n" +
                "import datetime\n" +
                "import time\n" +
                "from pyspark.sql.functions import md5, concat_ws\n" +
                "from pyspark.sql import types as spark_type\n" +
                "from pyspark import StorageLevel\n" +
                "\n" +
                "\n" +
                "# 数据增量抽取需要做ODS层的全量修复：ff表\n" +
                "# ODS层全量修复\n" +
                "# 定期(每周/每月)做一次全量的修复\n" +
                "# 具体步骤： 根据业务主键或者其他辅助字段，删除时间最长部分的数据\n" +
                "# 备注： ODS层需要保留一份全量数据\n" +
                "\n" +
                "\n" +
                "def snow_uuid(ori_id):\n" +
                "    epoch_2000 = 946656000\n" +
                "    cur_time = int((time.time() - epoch_2000) / 60)\n" +
                "    # timestamp 26 bit, partition 14 bit, seq id 30 bit\n" +
                "    generate_id = str((cur_time << 44) + ((ori_id >> 33) << 30) + (((1 << 33) - 1) & ori_id))\n" +
                "    return generate_id\n" +
                "\n" +
                "\n" +
                "if __name__ == '__main__':\n" +
                "    spark = pyspark.sql.SparkSession.builder.appName(\"m表名称_all_clean_fix\").enableHiveSupport().getOrCreate()\n" +
                "\n" +
                "    spark.sql(\"set hive.exec.dynamic.partition.mode = nonstrict\")\n" +
                "\n" +
                "    # ************************ff表修复逻辑****************************\n" +
                "    # 将f表数据插入ff表,这里的是全量抽取的f表\n" +
                "    # 固定同一批次的插入表的dl_loaded_dt时间,dl_start_dt以及dl_end_dt 相同\n" +
                "    fix_time_same_batch = spark.sql(\"\"\" select date_format(current_timestamp,\n" +
                "                                                'yyyy-MM-dd HH:mm:ss')\"\"\").first()[0]\n" +
                "    # 先清空ff表\n" +
                "    spark.sql(\"\"\" truncate table dl_scdp.ff的表名称\"\"\")\n" +
                "    spark.sql(\"\"\"select \n" +
                "                    你的字段,\n" +
                "                    '{}' as dl_loaded_dt,\n" +
                "                    date_format(current_date,'d') as day \n" +
                "                    from  dl_scdp.f表名称 a\"\"\".format(fix_time_same_batch)) \\\n" +
                "        .write.insertInto('dl_scdp.ff的表名称')\n" +
                "\n" +
                "    # ************************全量修复逻辑****************************\n" +
                "    spark.udf.register(\"snow_uuid\", snow_uuid)\n" +
                "\n" +
                "    # 此时需要保证ff表确实为最新的全量表\n" +
                "    df_N = spark.sql(\"\"\" select * from dl_scdp.ff的表名称 \"\"\")\n" +
                "    df_O = spark.sql(\"\"\" select * from dl_scdp.m表名称 where dl_status='A' \"\"\")\n" +
                "\n" +
                "    # 获取字段列表并删除系统功能字段，用于生成MD5\n" +
                "    # dl_check_key 用于识别唯一一条数据\n" +
                "    # 重分区促使数据均匀分布\n" +
                "    schema = df_N.schema\n" +
                "    # 去除ff表中中台添加的字段\n" +
                "    columns = [column for column in df_N.columns]\n" +
                "    columns.remove('day')\n" +
                "    columns.remove('dl_loaded_dt')\n" +
                "\n" +
                "    # 逻辑顺序： (为保证数据的交换速度，)\n" +
                "    #  1.先把N的数据全部抽取出来经过比对后作为新增建立dataframe插入新增表，比对的方式是：dc_check_key 相同则视为同一条数据\n" +
                "    #  2.选取O的dl_id和checksum两列建立临时表和O进行关联(关联条件是dl_id)，\n" +
                "    #   相同的就是更新的部分，将O的这部分查询出来将状态改为H，插入新增临时表\n" +
                "    #  3. 其余顺从逻辑走即可\n" +
                "\n" +
                "    # repartition 值设定一般为300-500，如果数据量很大可以调更大\n" +
                "    df_N = df_N.withColumn(\"dl_checksum\", md5(concat_ws('', *columns))) \\\n" +
                "        .withColumn(\"dl_check_key\", concat_ws('', '主键Id1-n', 'dl_checksum')) \\\n" +
                "        .rdd.keyBy(lambda row: row['dl_check_key']) \\\n" +
                "        .repartition(300)\n" +
                "\n" +
                "    df_O.withColumn(\"dl_checksum\", md5(concat_ws('', *columns))) \\\n" +
                "        .withColumn(\"dl_check_key\", concat_ws('', '主键Id1-n', 'dl_checksum')) \\\n" +
                "        .select('dl_check_key', 'dl_id') \\\n" +
                "        .rdd.map(lambda row: (row['dc_check_key'], row)) \\\n" +
                "        .repartition(300)\n" +
                "\n" +
                "    # 缓存数据，至少一千万\n" +
                "    df_O.setName('m表名称_df_O')\n" +
                "    df_O.persist(StorageLevel.MEMORY_AND_DISK)\n" +
                "    df_N.setName('m表名称_df_N')\n" +
                "    df_N.persist(StorageLevel.MEMORY_AND_DISK)\n" +
                "\n" +
                "    # 新集合 - 旧集合=N， 作为新增数据,写入到一张临时表，*****后面清洗*****完毕后写入m表\n" +
                "    # asDict转为{字段1：值1，字段2：值2} 形式\n" +
                "    subtract_df_N = df_N.subtractByKey(df_O, 300).map(lambda row: row[1].asDict(True))\n" +
                "    spark.createDataFrame(subtract_df_N, schema).write.saveAsTable('tmp_dl.m表名称_df_n')\n" +
                "\n" +
                "    # 旧集合-新集合=D，作为删除的数据\n" +
                "    df_O_Dl_Id = df_O.subtractByKey(df_N, 300).map(lambda row: {'dl_id': row[1]['dl_id']})\n" +
                "    # schema = spark_type.StructType([spark_type.StructField(\"dl_id\", spark_type.StringType(), True)])\n" +
                "    spark.createDataFrame(df_O_Dl_Id, df_O.schema).createOrReplaceTempView('tmp_dl.m表名称_d_dl_id')\n" +
                "\n" +
                "    # 真正的确认已经删除了的数据,需要将dl_status，day，dl_end_dt，\n" +
                "    subtract_df_D = spark.sql(\"\"\" \n" +
                "                   select \n" +
                "                        a.dl_id,\n" +
                "                        a.你的字段,\n" +
                "                        '{}' as dl_loaded_dt,\n" +
                "                        a.dl_start_dt,\n" +
                "                        '{}' as dl_end_dt,\n" +
                "                        'H' as dl_status,\n" +
                "                        date_format(current_date,'d') as day  \n" +
                "                        from dl_scdp.m表名称 a\n" +
                "                        inner join tmp_dl.m表名称_dl_id b\n" +
                "                        on a.uuid = b.uuid \n" +
                "                        where a.dl_status = 'A'\"\"\".format(fix_time_same_batch,fix_time_same_batch))\\\n" +
                "        .write.saveAsTable('tmp_dl.m表名称_df_d')\n" +
                "\n" +
                "    # 处理相同的数据：即中间的交叉部分，用O-D=中间交叉部分，需要保留(Keep)\n" +
                "    spark.sql(\"\"\" select \n" +
                "                            你的字段\n" +
                "                            from dl_scdp.m表名称 a\n" +
                "                            left join tmp_dl.表名称_dl_id b\n" +
                "                            on a.uuid = b.uuid \n" +
                "                            where a.dl_status = 'A' and b.uuid is null\n" +
                "                    \"\"\").write.saveAsTable('tmp_dl.m表名称_df_K')\n" +
                "\n" +
                "    # 清空m表A分区，以备数据插入\n" +
                "    spark.sql(\"\"\" ALTER TABLE dl_scdp.m表名称 drop if exists\n" +
                "                                       partition(dl_status='A')\"\"\")\n" +
                "\n" +
                "    spark.sql(\"\"\" select 你的字段, \n" +
                "                           from tmp_dl.m表名称_df_d \"\"\").write.insertInto('dl_scdp.m表名称')\n" +
                "\n" +
                "    spark.sql(\"\"\" select 你的字段 from tmp_dl.tmp_dl.m表名称_df_K \"\"\") \\\n" +
                "        .write.insertInto('dl_scdp.m表名称')\n" +
                "\n" +
                "    # 将新增数据经过处理后写入m表\n" +
                "    spark.sql(\"\"\" select \n" +
                "                       你的字段, \n" +
                "                       from tmp_dl.m表名称_df_n \"\"\")\\\n" +
                "        .filter('A 字段 is not null ...')\\\n" +
                "        .write.insertInto('dl_scdp.m表名称')\n" +
                "\n" +
                "    # 删除临时表\n" +
                "    spark.sql(\"\"\" drop table tmp_dl.tmp_dl.m表名称_df_K \"\"\")\n" +
                "    spark.sql(\"\"\" drop table tmp_dl.tmp_dl.m表名称_df_n \"\"\")\n" +
                "    spark.sql(\"\"\" drop table tmp_dl.tmp_dl.m表名称_df_d \"\"\")\n" +
                "\n" +
                "    spark.stop()\n";

        String pyfile="# -*- coding: utf-8 -*-\n" +
                "import datetime\n" +
                "\n" +
                "import pyspark\n" +
                "import time\n" +
                "\n" +
                "# 普通新增\n" +
                "# 假设湖里面的数据是：O（现阶段未失效的数据），新增数据为N\n" +
                "# O并集N表示=更新（U） （需要将该部分进行修复）\n" +
                "# 将O的U部分置为失效OH 写入原表O\n" +
                "# 最终数据=OH+N     其中除状态外其余数据一致部分为更新\n" +
                "# 大纲顺序：从f表获取数据到ff表，再从ff表获取数据到m表\n" +
                "\n" +
                "\n" +
                "if __name__ == '__main__':\n" +
                "    spark = pyspark.sql.SparkSession.builder.appName(\"m表名称_update_normal\").enableHiveSupport().getOrCreate()\n" +
                "\n" +
                "    # 普通新增\n" +
                "    # 假设湖里面的数据是：O（现阶段未失效的数据），新增数据为N 写入临时表T1\n" +
                "    # O并集N表示=更新（U） （需要将该部分进行修复）\n" +
                "    # O-U=未失效部分 写入临时表T2\n" +
                "    # 将O的U部分置为失效OH 写入临时表 T3\n" +
                "    # 最终数据=OH+N+O中未失效部分\n" +
                "\n" +
                "    # dl_loaded_dt：数据湖内创建时间)\n" +
                "    # 新增加 day字段，用于区分是哪天存储的\n" +
                "    # 步骤含义：将f表数据导入到ff表中\n" +
                "    # 固定同一批次的插入ff表的dl_loaded_dt时间\n" +
                "    fix_time_same_batch = spark.sql(\"\"\" select date_format(current_timestamp,'yyyy-MM-dd HH:mm:ss') \"\"\").first()[0]\n" +
                "    spark.sql(\"\"\"select \n" +
                "                    你的字段,\n" +
                "                    '{}' as dl_loaded_dt,\n" +
                "                    date_format(current_date,'d') as day \n" +
                "                    from  dl_scdp.f表名称 a\"\"\".format(fix_time_same_batch)\n" +
                "              ).write.insertInto('dl_scdp.ff的表名称')\n" +
                "\n" +
                "    # 将ff表的最新数据存储到m表，抽取大于ff表的最大dl_loaded_dt,这个时间需要根据上一次的成功时间确定\n" +
                "    ff_loaded_dt_most = spark.sql(\"\"\"select date_format(max(dl_loaded_dt),'yyyy-MM-dd HH:mm:ss') \n" +
                "       from dl_scdp.ff的表名称 a\"\"\").first()[0]\n" +
                "\n" +
                "    # 增量数据保留30天\n" +
                "    # 步骤含义：筛选需要更新到m表的数据，且清洗成功后，存在 tmp_dl.表名称_analysis_tmp\n" +
                "    # CREATED_BY        非空\n" +
                "    # CREATION_DATE     非空\n" +
                "    # LAST_UPDATE_DATE  非空\n" +
                "    # LAST_UPDATED_BY   非空\n" +
                "    # ORGANIZATION_ID   非空\n" +
                "    # PLANNER_CODE      非空\n" +
                "    # 插入m表时，需要填充数据的 dl_start_dt，dl_end_dt，dl_status，dl_id\n" +
                "    # tmp_dl 是数据湖专有分析临时表以及公共库，用完了记得删除，不做维护\n" +
                "    # spark.sql(\"\"\"select  dl_end_dt,dl_status,day,dl_id\n" +
                "    # from tmp_dl.pdc_m_mtl_planners_analysis_tmp limit 10\"\"\").show()\n" +
                "\n" +
                "    # 将数据处理完毕后写入临时表作为新增数据\n" +
                "    spark.sql(\"\"\" select \n" +
                "                    snow_uuid(a.ori_id) as dl_id,\n" +
                "                    a.你的字段,\n" +
                "                    '{}' as dl_loaded_dt,\n" +
                "                    '{}' as dl_start_dt,\n" +
                "                    '9999-12-31 00:00:00' as dl_end_dt, \n" +
                "                    date_format(current_date,'d') as day,\n" +
                "                    'A' as dl_status\n" +
                "                     from( \n" +
                "                         select  monotonically_increasing_id() as ori_id,\n" +
                "                        b.*\n" +
                "                        from dl_scdp.ff的表名称 b  where b.dl_loaded_dt='{}'\n" +
                "                        ) a \"\"\".format(fix_time_same_batch,fix_time_same_batch,ff_loaded_dt_most)).filter(\"A字段 is not null\") \\\n" +
                "        .filter(\"B字段 is not null\") \\\n" +
                "        .filter(\"其他的处理...\") \\\n" +
                "        .write.saveAsTable('tmp_dl.m表名称_analysis_tmp', mode='overwrite')\n" +
                "\n" +
                "    # 将处理完毕后的数据N和m表数据进行比对，将m表数据和N重合部分(U)置为失效\n" +
                "    # 插入m表时，需要填充数据的 dl_start_dt，dl_end_dt，dl_status\n" +
                "    spark.sql(\"\"\"select \n" +
                "                           m.dl_id,\n" +
                "                           m.你的字段,\n" +
                "                           '{}' as dl_loaded_dt,\n" +
                "                           m.dl_start_dt,\n" +
                "                           '{}' as dl_end_dt,\n" +
                "                           'H'  as dl_status,\n" +
                "                           date_format(current_date,'d') as day  \n" +
                "                        from dl_scdp.m表名称 m\n" +
                "                        inner join tmp_dl.m表名称_analysis_tmp n\n" +
                "                        on m.业务主键=n.业务主键 and 其他连接\n" +
                "                        where m.dl_status='A' \"\"\".format(fix_time_same_batch,fix_time_same_batch)).write.saveAsTable('tmp_dl.m表名称_updated_tmp', mode='overwrite')\n" +
                "\n" +
                "    count = spark.sql(\"\"\"select count(*) from tmp_dl.m表名称_updated_tmp\"\"\").first()[0]\n" +
                "\n" +
                "    # 如果确实有交叉部分，需要将原表的交叉部分剔除\n" +
                "    if count > 0:\n" +
                "        spark.sql(\"\"\"select m.*  from dl_scdp.m表名称 m\n" +
                "                          left join tmp_dl.m表名称_analysis_tmp n\n" +
                "                          on m.业务主键=n.业务主键 and 其他连接\n" +
                "                          where m.dl_status='A' and n.dl_id is null\"\"\")\\\n" +
                "            .write.saveAsTable('tmp_dl.m表名称_keep_tmp', mode='overwrite')\n" +
                "\n" +
                "        # 清空m表A分区，以备数据插入\n" +
                "        spark.sql(\"\"\" ALTER TABLE dl_scdp.m表名称 drop if exists\n" +
                "                            partition(dl_status='A')\"\"\")\n" +
                "\n" +
                "        spark.sql(\"\"\" select 你的字段 from tmp_dl.m表名称_keep_tmp \"\"\") \\\n" +
                "            .write.insertInto('dl_scdp.m表名称')\n" +
                "\n" +
                "        spark.sql(\"\"\" select 你的字段 from tmp_dl.m表名称_updated_tmp \"\"\") \\\n" +
                "            .write.insertInto('dl_scdp.m表名称')\n" +
                "\n" +
                "        spark.sql(\"\"\" drop table tmp_dl.m表名称_keep_tmp\"\"\")\n" +
                "\n" +
                "    # 将新增数据插入m表\n" +
                "    spark.sql(\"\"\" select 你的字段 from tmp_dl.m表名称_analysis_tmp \"\"\") \\\n" +
                "        .write.insertInto('dl_scdp.m表名称')\n" +
                "\n" +
                "    # 清理临时表\n" +
                "    spark.sql(\"\"\" drop table tmp_dl.m表名称_analysis_tmp\"\"\")\n" +
                "    spark.sql(\"\"\" drop table tmp_dl.tmp_dl.m表名称_updated_tmp\"\"\")\n" +
                "\n" +
                "    spark.stop()\n";
        String ftableNames="scdp_f_apps_h3c_wip_for_scdp_v,scdp_f_apps_mrp_item_sourcing_levels_v,scdp_f_erp_apps_h3c_isp_cmp_oppo_for_scdp_v,scdp_f_erp_apps_h3c_isp_cmp_wipc_for_scdp_v,scdp_f_erp_apps_h3c_po_asl_for_scdp_v,scdp_f_fnd_lookup_values_vl,scdp_f_h3c_bom_exploder_bom_data,scdp_f_h3c_it_assembly_materials_mv,scdp_f_hwcust_h3c_vmi_reservations,scdp_f_inv_mtl_material_transactions,scdp_f_inv_mtl_material_transactions_temp,scdp_f_inv_mtl_onhand_quantities_detail,scdp_f_inv_mtl_system_items_b,scdp_f_mrp_gross_requirements,scdp_f_mtl_reservations,scdp_f_mtl_secondary_inventories,scdp_f_mtl_supply,scdp_f_per_all_people_f,scdp_f_pmp_sc_production,scdp_f_pmp_sp_viewreserveplan,scdp_f_pmpquery_h3c_isp_genoppo_new_v,scdp_f_po_vendors,scdp_f_user010,scdp_f_viewfcvmilocksetting,scdp_f_viewiteminflogicmappingforscdp,scdp_f_wip_requirement_operations";
       // String ftableNames="scdp_f_fnd_lookup_values_vl,scdp_f_h3c_bom_exploder_bom_data,scdp_f_h3c_it_assembly_materials_mv,scdp_f_hwcust_h3c_vmi_reservations,scdp_f_inv_mtl_material_transactions,scdp_f_inv_mtl_material_transactions_temp,scdp_f_inv_mtl_onhand_quantities_detail,scdp_f_inv_mtl_system_items_b,scdp_f_mrp_gross_requirements,scdp_f_mtl_reservations,scdp_f_mtl_secondary_inventories,scdp_f_mtl_supply,scdp_f_per_all_people_f,scdp_f_pmp_sc_production,scdp_f_pmp_sp_viewreserveplan,scdp_f_pmpquery_h3c_isp_genoppo_new_v,scdp_f_po_vendors,scdp_f_user010,scdp_f_viewfcvmilocksetting,scdp_f_viewiteminflogicmappingforscdp,scdp_f_wip_requirement_operations";
        String fftableNames = "scdp_ff_fnd_lookup_values_vl,scdp_ff_h3c_bom_exploder_bom_data,scdp_ff_h3c_it_assembly_materials_mv,scdp_ff_hwcust_h3c_vmi_reservations,scdp_ff_inv_mtl_material_transactions,scdp_ff_inv_mtl_material_transactions_temp,scdp_ff_inv_mtl_onhand_quantities_detail,scdp_ff_inv_mtl_system_items_b,scdp_ff_mrp_gross_requirements,scdp_ff_mtl_reservations,scdp_ff_mtl_secondary_inventories,scdp_ff_mtl_supply,scdp_ff_per_all_people_f,scdp_ff_pmp_sc_production,scdp_ff_pmp_sp_viewreserveplan,scdp_ff_pmpquery_h3c_isp_genoppo_new_v,scdp_ff_po_vendors,scdp_ff_user010,scdp_ff_viewfcvmilocksetting,scdp_ff_viewiteminflogicmappingforscdp,scdp_ff_wip_requirement_operations";
        String mtableNames="scdp_m_fnd_lookup_values_vl,scdp_m_h3c_bom_exploder_bom_data,scdp_m_h3c_it_assembly_materials_mv,scdp_m_hwcust_h3c_vmi_reservations,scdp_m_inv_mtl_material_transactions,scdp_m_inv_mtl_material_transactions_temp,scdp_m_inv_mtl_onhand_quantities_detail,scdp_m_inv_mtl_system_items_b,scdp_m_mrp_gross_requirements,scdp_m_mtl_reservations,scdp_m_mtl_secondary_inventories,scdp_m_mtl_supply,scdp_m_per_all_people_f,scdp_m_pmp_sc_production,scdp_m_pmp_sp_viewreserveplan,scdp_m_pmpquery_h3c_isp_genoppo_new_v,scdp_m_po_vendors,scdp_m_user010,scdp_m_viewfcvmilocksetting,scdp_m_viewiteminflogicmappingforscdp,scdp_m_wip_requirement_operations";


        String[] ftableNameArr = ftableNames.split(",");
        String[] fftableNameArr = fftableNames.split(",");
        String[] mtableNameArr = mtableNames.split(",");


        for (int i = 0; i < ftableNameArr.length; i++) {
            String pyfileTemp=pyfile;
            String ftableName = ftableNameArr[i];
            String mtableName = "dl_scdp."+ftableName.replace("_f_", "_m_");
            //读取字段
            String s = FileUtil.readString(FileUtil.file("C:\\Users\\g27937\\Desktop\\入湖\\supply_demand_platform\\02方案设计\\入湖准备--请勿动\\基础准备\\"+ftableName+"\\"+ftableName+".sql"), Charset.forName("UTF-8"));
            String ziduan = s.substring(s.indexOf("--原表字段") + 6, s.length()).trim();

           // String pyFilename=ftableName+"_update_all_clean_fix.py";
           String pyFilename=ftableName+"_update.py";

            pyfileTemp=pyfileTemp.replaceAll("f表名称",ftableName);
            pyfileTemp=pyfileTemp.replaceAll("m表名称",mtableName);
            String fftableName = "dl_scdp."+ftableName.replace("_f_", "_ff_");
           // pyfile=pyfile.replaceAll("dl_scdp.ff的表名称",fftableName);
            pyfileTemp=pyfileTemp.replaceAll("你的字段,",ziduan);
            pyfileTemp=pyfileTemp.replaceAll("你的字段",ziduan);
            pyfileTemp=pyfileTemp.replaceAll("dl_scdp.ff的表名称",fftableName);

            //写入文件
            FileUtil.writeString(pyfileTemp,new File("C:\\Users\\g27937\\Desktop\\saple\\"+pyFilename),Charset.forName("UTF-8"));
        }


    }








    /**
     *
     * @param pyfile py文件模板内容
     * @param ftableNameList f表名称，多个表名称用英文逗号隔开
     * @param pyFilesuffix py文件后缀名称
     * @param ftableNamePath f表父级目录
     * @param pyFilePath 生成py文件目录
     * @param datebaseName 数据表名称
     */
    public void generateFileMethod(String pyfile,List<String> ftableNameList ,String pyFilesuffix,String ftableNamePath,String pyFilePath,String datebaseName) {


        for (int i = 0; i < ftableNameList.size(); i++) {
            String temp =pyfile;
            String ftableName = ftableNameList.get(i);
            String fftableName = ftableName.replace("_f_", "_ff_");
            String mtableName = ftableName.replace("_f_", "_m_");

            //读取字段
            String s = FileUtil.readString(FileUtil.file(ftableNamePath+File.separator+ftableName+File.separator+ftableName+".sql"), Charset.forName("UTF-8"));
            String ziduan = s.substring(s.indexOf("--原表字段") + 6, s.length()).trim();

            String pyFilename=ftableName+pyFilesuffix;

            temp=temp.replaceAll("f表名称",ftableName);
            temp=temp.replaceAll("m表名称",mtableName);
            temp=temp.replaceAll("你的字段,",ziduan);
            temp=temp.replaceAll("你的字段",ziduan);
            temp=temp.replaceAll("ff的表名称",fftableName);
            temp=temp.replaceAll("数据库",datebaseName);

            //写入文件
            FileUtil.writeString(temp,new File(pyFilePath+File.separator+pyFilename),Charset.forName("UTF-8"));
        }


    }



    @Test
    public void writeTablesql(){

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
                "ROW FORMAT DELIMITED fields terminated by '\\t'\n" +
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
        String tableNameprefix="scdp_f_";
        String field="";
        String fileType="";
        String excelName="ERP-APPS.H3C_ISP_CMP_WIPC_FOR_SCDP_V.xlsx";
        String path="C:\\Users\\g27937\\Desktop\\入湖\\supply_demand_platform\\02方案设计\\表整理"+File.separator+excelName;
        ExcelReader reader = ExcelUtil.getReader(FileUtil.file(path));
        List<Map<String, Object>> readAll = reader.readAll();

        //建表语句 字段加类型
        StringBuffer fieldAndtypeBuffer=new StringBuffer();
        //建表语句 原表字段
        StringBuffer fieldBuffer=new StringBuffer();
        readAll.stream().forEach(e->{
            String col_name = e.get("col_name").toString();
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
                col_type=col_type.replaceAll("date","timestamp");
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
            String col_desc = e.get("col_desc").toString();

            fieldAndtypeBuffer.append(col_name).append(" ").append(col_type).append(",").append("\n");
            fieldBuffer.append(col_name).append(",").append("\n");
        });
        f=SQLUtils.format(f.replaceAll("字段",fieldAndtypeBuffer.toString()), JdbcConstants.HIVE);
        ff=SQLUtils.format(ff.replaceAll("字段",fieldAndtypeBuffer.toString()), JdbcConstants.HIVE);
        m=SQLUtils.format(m.replaceAll("字段",fieldAndtypeBuffer.toString()), JdbcConstants.HIVE);
        //替换表名称
        content=content.replaceAll("HIVE建表：","HIVE建表："+"\n"+f);
        content=content.replaceAll("spark建立ff表：","spark建立ff表："+"\n"+ff);
        content=content.replaceAll("spark建立m表：","spark建立m表："+"\n"+m);
        content=content.replaceAll("表名称",tableName);
        content=content+"\n"+fieldBuffer.toString();
        //写入文件
        FileUtil.writeString(content,new File("C:\\Users\\g27937\\Desktop\\入湖\\supply_demand_platform\\02方案设计\\入湖准备--请勿动\\基础准备"+File.separator+tableNameprefix+tableName+File.separator+tableNameprefix+tableName+".sql"),Charset.forName("UTF-8"));


    }

    @Test
    public void asda(){
        System.out.println( ReUtil.contains("[varchar\\(][0-9][\\)]","varchar(4)"));

    }








}
