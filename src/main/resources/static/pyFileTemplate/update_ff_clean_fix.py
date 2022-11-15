import pyspark
from pyspark.sql.functions import md5, concat_ws

# 数据增量抽取需要做ODS层的全量修复：ff表
# ODS层全量修复
# 定期(每周/每月)做一次全量的修复
# 具体步骤： 根据业务主键或者其他辅助字段，删除时间最长部分的数据
# 备注： ODS层需要保留一份全量数据


if __name__ == '__main__':
    spark = pyspark.sql.SparkSession.builder.appName("ff的表名称").enableHiveSupport().getOrCreate()

    spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")

    # 获取表字段信息
    pre_df = spark.sql(""" select * from 数据库.ff的表名称 """)
    columns = [column for column in pre_df.columns]
    columns.remove('day')
    columns.remove('dl_loaded_dt')
    # 选取最新的一条,concat_ws中的字段的作用是：用于区分本条数据和其他数据，即本条数据独有的
    # eg：concat_ws('', 'service_contract_id', 'version', 'dc_checksum'))

    pre_df = pre_df.withColumn("dc_checksum", md5(concat_ws('', *columns))) \
        .withColumn("dc_check_key", concat_ws('', '主键Id1-n', 'dc_checksum')) \
        .write.saveAsTable('tmp_dl.ff的表名称_fix_tmp')

    new_df = spark.sql("""
            select 你的字段,
                   day,
                   dl_loaded_dt      
           row_number() over(partition by dc_check_key order by 业务更新时间戳字段 desc) as rn 
          from tmp_dl.ff的表名称_fix_tmp where rn<2
      """)

    # 清空原表
    spark.sql(""" truncate table 数据库.ff的表名称""")

    # 将最新的数据写入原表
    new_df.write.insertInto('数据库.ff的表名称')

    spark.stop()
