from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, first


def Extract_Popular_Images(d: DataFrame, limit: int, min_width: int, min_height: int):
    df = d.select("url", "md5")
    count = df.groupBy("md5").count()

    return (
        df.join(count, "md5")
        .groupBy("md5")
        .agg(first("url").alias("url"), first("count").alias("count"))
        .select("url", "count")
        .orderBy(desc("count"))
        .limit(limit)
    )


def Write_Gexf(d, path, sc):
    return sc._jvm.io.archivesunleashed.app.WriteGEXF(d, path).apply


def Write_Graphml(d, path, sc):
    return sc._jvm.io.archivesunleashed.app.WriteGraphML(d, path).apply
