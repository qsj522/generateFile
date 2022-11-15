# -*- coding: utf-8 -*-
import pyspark
import time


def snow_uuid(ori_id):
    epoch_2000 = 946656000
    cur_time = int((time.time() - epoch_2000) / 60)
    # timestamp 26 bit, partition 14 bit, seq id 30 bit
    generate_id = str((cur_time << 44) + ((ori_id >> 33) << 30) + (((1 << 33) - 1) & ori_id))
    return generate_id


# 全量抽取的模板，dl_scdp为数据库名，如果不是该数据库也需要做替换
if __name__ == '__main__':

    spark = pyspark.sql.SparkSession.builder.appName("m表名称_all").enableHiveSupport().getOrCreate()

    # 将填充ff表的数据的失效时间,（数据保留七天）并且去除掉上周的时间（如：今天周三，where过滤上周三的数据）
    #  步骤含义：按照天分区，所以直接先删除原分区即可
    # 如果一天内多次导入，需要保留该天的以前版本
    day = spark.sql("""select date_format(current_date,'u') as day """).first()[0]

    # 检查大于当天0点前是否有数据
    today_count = spark.sql(""" select count(*) from 数据库.ff的表名称 
        where day={} and dl_loaded_dt > (select date_format(current_date,'yyyy-MM-dd 00:00:00'))""".format(day)).first()[0]
    if today_count == 0:
        spark.sql(""" ALTER TABLE 数据库.ff的表名称 drop if exists
                    partition(day={})""".format(day))

    spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
    spark.udf.register("snow_uuid", snow_uuid)

    # dl_loaded_dt：数据湖内创建时间)
    # 新增加 day字段，用于区分是哪天存储的，字段是你自己表所有的字段
    # 步骤含义：将f表dl_loaded_dt数据导入到ff表中

    # 固定同一批次的插入表的dl_loaded_dt时间,dl_start_dt以及dl_end_dt 相同
    fix_time_same_batch = spark.sql(""" select date_format(current_timestamp,'yyyy-MM-dd HH:mm:ss') """).first()[0]
    spark.sql("""select 
                    你的字段,
                    '{}' as dl_loaded_dt,
                    date_format(current_date,'u') as day 
                    from  数据库.f表名称""".format(fix_time_same_batch)) \
        .write.insertInto('数据库.ff的表名称')

    # 顺序：从ff表获取合格数据清洗后存储到临时表做，再将原m表的数据做处理（将原A状态改为H，将原（H day）分区删除）再将数据插入

    # 将ff表的最新数据存储到m表，抽取大于ff表的最大dl_loaded_dt
    ff_loaded_dt_most = spark.sql("""select date_format(max(dl_loaded_dt),'yyyy-MM-dd HH:mm:ss') 
    from 数据库.ff的表名称 a""").first()[0]

    # 步骤含义：筛选需要更新到m表的数据，且清洗成功后，存在 tmp_dl.pdc_m_mtl_planners_analysis_tmp
    # CREATED_BY        非空
    # CREATION_DATE     非空
    # LAST_UPDATE_DATE  非空
    # LAST_UPDATED_BY   非空
    # ORGANIZATION_ID   非空
    # PLANNER_CODE      非空
    # 插入m表时，需要填充数据的 dl_start_dt，dl_end_dt，dl_status，dl_id
    # tmp_dl 是数据湖专有分析临时表以及公共库，用完了记得删除，不做维护

    # 固定同一批次的插入m表的dl_loaded_dt,dl_start_dt时间
    spark.sql(""" select 
                    snow_uuid(a.ori_id) as dl_id,
                    a.的字段
                    '{}' as dl_loaded_dt,
                    '{}' as dl_start_dt,
                    '9999-12-31 00:00:00' as dl_end_dt, 
                    date_format(current_date,'u') as day,
                    'A' as dl_status
                    from( 
                     select  monotonically_increasing_id() as ori_id,
                    b.*
                    from 数据库.ff的表名称 b  where b.dl_loaded_dt >= '{}'
                    ) a """.format(fix_time_same_batch, fix_time_same_batch, ff_loaded_dt_most)).filter().write.saveAsTable('tmp_dl.m表名称_analysis_tmp', mode='overwrite')

    # a.dl_loaded_dt,
    # a.dl_start_dt,
    # nvl(a.dl_end_dt, current_timestamp) as dl_end_dt,
    # a.dl_status,
    # 写入ff表之前需要将中台所需要的字段填充完毕
    # dl_id：数据唯一ID3)
    # dl_status：数据状态4)，值域如下：
    # A：可用数据
    # N：临时状态
    # P：全量快照备份数据
    # H：增量历史版本数据
    # dl_loaded_dt：数据湖内创建时间5)
    # dl_start_dt：数据开始生效时间6)
    # dl_end_dt：数据失效时间，未失效值默认设置为9999-12-31 00:00:00)

    # 按照H和day分区
    # 顺序：从ff表获取合格数据清洗后存储到临时表做，再将原m表的数据做处理（将原（P day）分区删除，将原（A day）状态改为(p day)再将数据插入

    # 删除上周的H分区
    spark.sql(""" ALTER TABLE 数据库.m表名称 drop if exists
                                    partition(dl_status='P', day={} )""".format(day))

    # (A day) 改为(P day) 并存储在临时表
    spark.sql(""" select 
                       a.dl_id,
                       a.的字段
                       '{}' as dl_loaded_dt,
                        a.dl_start_dt,
                       '{}' as dl_end_dt,
                       'P'  as dl_status,
                       date_format(current_date,'u') as day  
                       from 数据库.m表名称 a where a.dl_status='A'"""
              .format(fix_time_same_batch, fix_time_same_batch)) \
        .write.saveAsTable("tmp_dl.m表名称_tmp", mode='overwrite')

    # 将原A分区修改后的数据写入m表的H分区中
    spark.sql("select * from tmp_dl.m表名称_tmp").write.insertInto('数据库.m表名称')

    # 删除原可用分区A：
    spark.sql(""" ALTER TABLE 数据库.m表名称 drop if exists
                           partition(dl_status='A')""")

    # 将清洗完毕后数据插入m表
    spark.sql("""select 
                    a.dl_id,
                    a.的字段
                    a.dl_loaded_dt,
                    a.dl_start_dt,
                    a.dl_end_dt, 
                    a.dl_status,
                    a.day
                    from tmp_dl.m表名称_analysis_tmp a""") \
        .write.insertInto('数据库.m表名称')

    # 临时表清理
    spark.sql(""" drop table if exists tmp_dl.m表名称_tmp""")
    spark.sql(""" drop table if exists tmp_dl.m表名称_analysis_tmp""")

    spark.stop()
