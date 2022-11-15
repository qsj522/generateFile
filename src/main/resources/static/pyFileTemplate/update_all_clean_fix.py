import pyspark
import datetime
import time
from pyspark.sql.functions import md5, concat_ws
from pyspark.sql import types as spark_type
from pyspark import StorageLevel


# 数据增量抽取需要做ODS层的全量修复：ff表**********************全量修复*********
# ODS层全量修复
# 定期(每周/每月)做一次全量的修复
# 具体步骤： 根据业务主键或者其他辅助字段，删除时间最长部分的数据
# 备注： ODS层需要保留一份全量数据


def snow_uuid(ori_id):
    epoch_2000 = 946656000
    cur_time = int((time.time() - epoch_2000) / 60)
    # timestamp 26 bit, partition 14 bit, seq id 30 bit
    generate_id = str((cur_time << 44) + ((ori_id >> 33) << 30) + (((1 << 33) - 1) & ori_id))
    return generate_id


if __name__ == '__main__':
    spark = pyspark.sql.SparkSession.builder.appName("m表名称_all_clean_fix").enableHiveSupport().getOrCreate()

    spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")

    # ************************ff表修复逻辑****************************
    # 将f表数据插入ff表,这里的是全量抽取的f表
    # 固定同一批次的插入表的dl_loaded_dt时间,dl_start_dt以及dl_end_dt 相同
    fix_time_same_batch = spark.sql(""" select date_format(current_timestamp,
                                                'yyyy-MM-dd HH:mm:ss')""").first()[0]
    # 先清空ff表
    spark.sql(""" truncate table 数据库.ff的表名称""")
    spark.sql("""select 
                    你的字段,
                    '{}' as dl_loaded_dt,
                    date_format(current_date,'d') as day 
                    from  dl_scdp.f表名称 a""".format(fix_time_same_batch)) \
        .write.insertInto('dl_scdp.ff的表名称')

    # ************************全量修复逻辑****************************
    spark.udf.register("snow_uuid", snow_uuid)

    # 此时需要保证ff表确实为最新的全量表
    df_N = spark.sql(""" select * from 数据库.ff的表名称 """)
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
    spark.createDataFrame(df_O_Dl_Id, schema).write.saveAsTable('tmp_dl.m表名称_d_dl_id',mode='overwrite')

    # 真正的确认已经删除了的数据,需要将dl_status，day，dl_end_dt，
    subtract_df_D = spark.sql(""" 
                   select 
                        a.dl_id,
                        a.你的字段,
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
                            left join tmp_dl.m表名称_d_dl_id b
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
                            ) a """.format(fix_time_same_batch, fix_time_same_batch)).filter().write.insertInto('数据库.m表名称')

    # 删除临时表
    spark.sql(""" drop table tmp_dl.m表名称_df_K """)
    spark.sql(""" drop table tmp_dl.m表名称_df_n """)
    spark.sql(""" drop table tmp_dl.m表名称_df_d """)

    spark.stop()
