# -*- coding: utf-8 -*-
import pyspark
import time
import datetime
from pyspark.sql.functions import md5, concat_ws
from pyspark.sql import types as spark_type
from pyspark import StorageLevel


# **************************************全量修复****************
# 增量抽取的数据的全量修复需要做考虑以下几种情况：
# 1.增量接入
# 2.原系统有删除的数据
# 3.原系统有更新的数据
# 4.综合1.2.3
# 5.需要做到版本的追溯

# 假设湖里面的数据是：O（现阶段未失效的数据），新增数据为N
# 则：N-O=新增数据 （新数据有，但是老数据没有，所以是新增的）
# 则：O-N=失效数据(D) （老数据有，但是新数据没有的，就是不需要的：失效数据,需要改变状态：A->H）
# O并集N表示=更新（U） （新老数据重合，意味着本部分数据在原系统进行了更新）
# 将O的U部分(OH)置为失效写入临时表，注意：需要更新失效时间
# 最终数据(存储在临时表里)=D+OH+N
#  清空O表，将临时表数据插入O表

# 逻辑顺序：全量抽取ff表作为N 进行比较

def snow_uuid(ori_id):
    epoch_2000 = 946656000
    cur_time = int((time.time() - epoch_2000) / 60)
    # timestamp 26 bit, partition 14 bit, seq id 30 bit
    generate_id = str((cur_time << 44) + ((ori_id >> 33) << 30) + (((1 << 33) - 1) & ori_id))
    return generate_id


if __name__ == '__main__':
    spark = pyspark.sql.SparkSession.builder.appName("m表名称_update_all_fix").enableHiveSupport().getOrCreate()

    spark.udf.register("snow_uuid", snow_uuid)
    spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")

    # ************************全量修复逻辑****************************
    fix_time_same_batch = spark.sql(""" select date_format(current_timestamp,
                                                    'yyyy-MM-dd HH:mm:ss')""").first()[0]

    # 此时需要保证ff表确实为最新的全量表
    df_N = spark.sql(""" select * from 数据库.ff表名称 """)
    df_O = spark.sql(""" select * from 数据库.m表名称 where dl_status='A' """)

    # 获取字段列表并删除系统功能字段，用于生成MD5
    # dl_check_key 用于识别唯一一条数据
    # 重分区促使数据均匀分布
    schema = df_N.schema
    # 去除ff表中中台添加的字段
    columns = [column for column in df_N.columns]
    columns.remove('day')
    columns.remove('dl_loaded_dt')

    # 逻辑顺序： (为保证数据的交换速度，)
    #  1.先把N的数据全部抽取出来经过比对后作为新增建立dataframe插入新增表，比对的方式是：dc_check_key 相同则视为同一条数据
    #  2.选取O的dl_id和checksum两列建立临时表和O进行关联(关联条件是dl_id)，
    #   相同的就是更新的部分，将O的这部分查询出来将状态改为H，插入新增临时表
    #  3. 其余顺从逻辑走即可

    # repartition 值设定一般为300-500，如果数据量很大可以调更大
    df_N = df_N.withColumn("dl_checksum", md5(concat_ws('', *columns))) \
        .withColumn("dl_check_key", concat_ws('', '主键Id1-n', 'dl_checksum')) \
        .rdd.keyBy(lambda row: row['dl_check_key']) \
        .repartition(300)

    df_O = df_O.withColumn("dl_checksum", md5(concat_ws('', *columns))) \
        .withColumn("dl_check_key", concat_ws('', '主键Id1-n', 'dl_checksum')) \
        .select('dl_check_key', 'dl_id') \
        .rdd.map(lambda row: (row['dc_check_key'], row)) \
        .repartition(300)

    # 缓存数据，至少一千万
    df_O.setName('m表名称_df_O')
    df_O.persist(StorageLevel.MEMORY_AND_DISK)
    df_N.setName('m表名称_df_N')
    df_N.persist(StorageLevel.MEMORY_AND_DISK)

    # 新集合 - 旧集合=N， 作为新增数据,写入到一张临时表，*****后面清洗*****完毕后写入m表
    # asDict转为{字段1：值1，字段2：值2} 形式
    subtract_df_N = df_N.subtractByKey(df_O, 300).map(lambda row: row[1].asDict(True))
    spark.createDataFrame(subtract_df_N, schema).write.saveAsTable('tmp_dl.m表名称_df_n')

    # 旧集合-新集合=D，作为删除的数据
    df_O_Dl_Id = df_O.subtractByKey(df_N, 300).map(lambda row: {'dl_id': row[1]['dl_id']})
    schema = spark_type.StructType([spark_type.StructField("dl_id", spark_type.StringType(), True)])
    spark.createDataFrame(df_O_Dl_Id, schema).write.saveAsTable('tmp_dl.m表名称_d_dl_id', mode='overwrite')

    # 真正的确认已经删除了的数据,需要将dl_status，day，dl_end_dt，
    subtract_df_D = spark.sql(""" 
                      select 
                           a.dl_id,
                           a.的字段,
                           '{}' as dl_loaded_dt,
                           a.dl_start_dt,
                           '{}' as dl_end_dt,
                           'H' as dl_status,
                           date_format(current_date,'d') as day  
                           from 数据库.m表名称 a
                           left join tmp_dl.m表名称_d_dl_id b
                           on a.dl_id = b.dl_id 
                           where a.dl_status = 'A'  and b.dl_id is not null """
                              .format(fix_time_same_batch, fix_time_same_batch)) \
        .write.saveAsTable('tmp_dl.m表名称_df_d')

    # 处理相同的数据：即中间的交叉部分，用O-D=中间交叉部分，需要保留(Keep)
    spark.sql(""" select 
                               dl_id,
                           你的字段,
                           day,
                           dl_loaded_dt,
                           dl_start_dt,
                           dl_end_dt,
                           dl_status 
                               from 数据库.m表名称 a
                               left join tmp_dl.表名称_dl_id b
                               on a.dl_id = b.dl_id 
                               where a.dl_status = 'A' and b.dl_id is null
                       """).write.saveAsTable('tmp_dl.m表名称_df_K')

    # 清空m表A分区，以备数据插入
    spark.sql(""" ALTER TABLE 数据库.m表名称 drop if exists
                                          partition(dl_status='A')""")

    spark.sql(""" select dl_id,
                           你的字段,
                           day,
                           dl_loaded_dt,
                           dl_start_dt,
                           dl_end_dt,
                           dl_status 
                              from tmp_dl.m表名称_df_d """).write.insertInto('数据库.m表名称')

    spark.sql(""" select dl_id,
                           你的字段,
                           day,
                           dl_loaded_dt,
                           dl_start_dt,
                           dl_end_dt,
                           dl_status  from tmp_dl.m表名称_df_K """) \
        .write.insertInto('数据库.m表名称')

    # 将新增数据经过处理后写入m表
    spark.sql(""" select 
                           snow_uuid(a.ori_id) as dl_id,
                           你的字段,
                           '{}' as dl_loaded_dt,
                           '{}' as dl_start_dt,
                           '9999-12-31 00:00:00' as dl_end_dt, 
                           date_format(current_date,'d') as day,
                           'A' as dl_status
                            from( 
                                select  monotonically_increasing_id() as ori_id,
                               b.*
                               from tmp_dl.m表名称_df_n b 
                               ) a """.format(fix_time_same_batch, fix_time_same_batch)) .filter() .write.insertInto('数据库.m表名称')

    # 删除临时表
    spark.sql(""" drop table tmp_dl.m表名称_df_K """)
    spark.sql(""" drop table tmp_dl.m表名称_df_n """)
    spark.sql(""" drop table tmp_dl.m表名称_df_d """)

    spark.stop()
