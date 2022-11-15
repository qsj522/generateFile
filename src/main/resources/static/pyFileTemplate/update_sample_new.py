# -*- coding: utf-8 -*-
import datetime

import pyspark
import time


# 普通新增
# 假设湖里面的数据是：O（现阶段未失效的数据），新增数据为N
# O并集N表示=更新（U） （需要将该部分进行修复）
# 将O的U部分置为失效OH 写入原表O
# 最终数据=OH+N     其中除状态外其余数据一致部分为更新
# 大纲顺序：从f表获取数据到ff表，再从ff表获取数据到m表

# /apps/hdfs/tmp_dl/dl_scdp 这个路径需要申请

def snow_uuid(ori_id):
    epoch_2000 = 946656000
    cur_time = int((time.time() - epoch_2000) / 60)
    # timestamp 26 bit, partition 14 bit, seq id 30 bit
    generate_id = str((cur_time << 44) + ((ori_id >> 33) << 30) + (((1 << 33) - 1) & ori_id))
    return generate_id


if __name__ == '__main__':
    spark = pyspark.sql.SparkSession.builder.appName("m表名称_update_normal").enableHiveSupport().getOrCreate()

    spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
    spark.udf.register("snow_uuid", snow_uuid)

    # dl_loaded_dt：数据湖内创建时间)
    # 新增加 day字段，用于区分是哪天存储的
    # 步骤含义：将f表数据导入到ff表中

    # 固定同一批次的插入ff表的dl_loaded_dt时间
    fix_time_same_batch = spark.sql(""" select date_format(current_timestamp,'yyyy-MM-dd HH:mm:ss') """).first()[0]
    day = spark.sql(""" select date_format(current_date,'d') as day""").first()[0]
    day = int(day)

    spark.sql("""select 
                    你的字段,
                     to_timestamp('{}') as dl_loaded_dt,
                    {} as day 
                    from  数据库.f表名称 a""".format(fix_time_same_batch, day)
              ).write.saveAsTable('tmp_dl.ff的表名称_tmp', mode='overwrite')

    # 增量数据保留30天
    # 步骤含义：筛选需要更新到m表的数据，且清洗成功后，存在 tmp_dl.表名称_analysis_tmp
    # eg：CREATED_BY        非空
    # 插入m表时，需要填充数据的 dl_start_dt，dl_end_dt，dl_status，dl_id
    # tmp_dl 是数据湖专有分析临时表以及公共库，用完了记得删除，不做维护
    # 将数据处理完毕后写入临时表作为新增数据
    # filter()用来做非空等条件的筛选
    spark.sql(""" select 
                    snow_uuid(a.ori_id) as dl_id,
                    你的字段,
                    to_timestamp('{}') as dl_loaded_dt,
                    to_timestamp('{}') as dl_start_dt,
                    to_timestamp('9999-12-31 00:00:00') as dl_end_dt, 
                    {} as day,
                    'A' as dl_status
                     from( 
                         select  monotonically_increasing_id() as ori_id,
                        b.*
                        from tmp_dl.ff的表名称_tmp b 
                        ) a """.format(fix_time_same_batch, fix_time_same_batch, day)) \
        .filter().write.saveAsTable('tmp_dl.m表名称_analysis_tmp', mode='overwrite')

    # 将新增数据插入m表的备份路径
    spark.sql(""" select dl_id,
                            你的字段,
                            day,
                            dl_loaded_dt,
                            dl_start_dt,
                            dl_end_dt,
                            dl_status 
                            from tmp_dl.m表名称_analysis_tmp """) \
        .write.format("parquet").mode("overwrite").save('/apps/hdfs/tmp_dl/dl_scdp/m表名称')

    # 将处理完毕后的数据N和m表数据进行比对，将m表数据和N重合部分(U)置为失效
    # 插入m表时，需要填充数据的 dl_start_dt，dl_end_dt，dl_status
    # 主_键：用来做连接用
    spark.sql("""select 
                           m.dl_id,
                           m.的字段
                          {} as day,
                           to_timestamp('{}') as dl_loaded_dt,
                            m.dl_start_dt,
                           to_timestamp('{}') as dl_end_dt,
                           'H'  as dl_status
                        from 数据库.m表名称 m
                        inner join tmp_dl.m表名称_analysis_tmp n
                        on 主键
                        where m.dl_status='A' """.format(day, fix_time_same_batch, fix_time_same_batch)) \
        .write.saveAsTable('tmp_dl.m表名称_updated_tmp', mode='overwrite')

    spark.sql(""" select dl_id,
                        你的字段,
                        day,
                        dl_loaded_dt,
                        dl_start_dt,
                        dl_end_dt,
                        dl_status  from tmp_dl.m表名称_updated_tmp """) \
        .write.format("parquet").mode("append").save('/apps/hdfs/tmp_dl/dl_scdp/m表名称')

    count = spark.sql("""select count(*) from tmp_dl.m表名称_updated_tmp""").first()[0]

    if count > 0:
        # 如果确实有交叉部分，需要将原表的交叉部分剔除,即：将需要保留的部分存入该路径下
        spark.sql("""select m.dl_id,
                            m.的字段
                            m.day,
                            m.dl_loaded_dt,
                            m.dl_start_dt,
                            m.dl_end_dt,
                            m.dl_status  
                            from 数据库.m表名称 m
                          left join tmp_dl.m表名称_updated_tmp n
                          on 主键
                          where m.dl_status='A' and n.dl_id is null""") \
            .write.format("parquet").mode("append").save('/apps/hdfs/tmp_dl/dl_scdp/m表名称')
        # 清空m表A分区，以备数据插入
        spark.sql(""" ALTER TABLE 数据库.m表名称 drop if exists
                                        partition(dl_status='A')""")

    # 最后处理视为成功的数据
    # 将ff表的临时数据N写到ff表保存，成功了再插入ff
    spark.sql("""select 
                       你的字段,
                        dl_loaded_dt,
                        day 
                       from  tmp_dl.ff的表名称_tmp """
              ).write.insertInto('数据库.ff的表名称')

    df = spark.sql("select * from 数据库.m表名称 limit 1")

    # 将该路径下的数据写入m表
    spark.read.format("parquet").load('/apps/hdfs/tmp_dl/dl_scdp/m表名称', schema=df.schema).write.insertInto("数据库.m表名称")

    # 清理临时表
    spark.sql(""" drop table tmp_dl.m表名称_updated_tmp""")
    spark.sql(""" drop table tmp_dl.m表名称_analysis_tmp""")
    spark.sql(""" drop table tmp_dl.ff的表名称_tmp""")
    spark.stop()
