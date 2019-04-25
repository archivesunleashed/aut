import argparse
import os
import zipfile
from pyspark import SparkConf, SparkContext, SQLContext


def init_spark(master, aut_jar):
    conf = SparkConf()
    conf.set("spark.jars", aut_jar)
    conf.set("spark.sql.execution.arrow.enabled", "true")
    conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1280")
    conf.set("spark.executor.memory", "16G")
    conf.set("spark.cores.max", "84")
    conf.set("spark.executor.cores", "6")
    conf.set("spark.driver.memory", "64G")
    conf.set("spark.task.cpus", "3")
    sc = SparkContext(master, "aut image analysis", conf=conf)
    sql_context = SQLContext(sc)
    return conf, sc, sql_context


def get_args():
    parser = argparse.ArgumentParser(description='PySpark for Web Archive Image Retrieval')
    parser.add_argument('--web_archive', help='input directory for web archive data', default='/tuna1/scratch/nruest/geocites/warcs')
    parser.add_argument('--aut_jar', help='aut compiled jar package', default='aut/target/aut-0.17.1-SNAPSHOT-fatjar.jar')
    parser.add_argument('--aut_py', help='path to python package', default='aut/src/main/python')
    parser.add_argument('--spark', help='path to python package', default='spark-2.3.2-bin-hadoop2.7/bin')
    parser.add_argument('--master', help='master IP address', default='spark://127.0.1.1:7077')
    parser.add_argument('--img_model', help='model for image processing, use ssd', default='ssd')
    parser.add_argument('--filter_size', nargs='+', type=int, help='filter out images smaller than filter_size', default=[640, 640])
    parser.add_argument('--output_path', help='image model output dir', default='coco_res')
    return parser.parse_args()


def zip_model_module(PYAUT_DIR):
    zip = zipfile.ZipFile(os.path.join(PYAUT_DIR, "tf", "model.zip"), "w")
    zip.write(os.path.join(PYAUT_DIR, "tf", "model", "__init__.py"), os.path.join("model", "__init__.py"))
    zip.write(os.path.join(PYAUT_DIR, "tf", "model", "object_detection.py"), os.path.join("model", "object_detection.py"))
    zip.write(os.path.join(PYAUT_DIR, "tf", "model", "preprocess.py"), os.path.join("model", "preprocess.py"))

