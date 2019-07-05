import os
import sys
from util.init import *
from model.object_detection import *
PYAUT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PYAUT_DIR)

from aut.common import WebArchive
from pyspark.sql import DataFrame


if __name__ == "__main__":
    # initialization
    args = get_args()
    sys.path.append(args.spark)
    conf, sc, sql_context = init_spark(args.master, args.aut_jar)
    zip_model_module(PYAUT_DIR)
    sc.addPyFile(os.path.join(PYAUT_DIR, "tf", "model.zip"))
    if args.img_model == "ssd":
        detector = SSD(sc, sql_context, args)

    # preprocessing raw images
    arc = WebArchive(sc, sql_context, args.web_archive)
    df = DataFrame(arc.loader.extractImages(arc.path), sql_context)
    filter_size = tuple(args.filter_size)
    print("height >= %d and width >= %d"%filter_size)
    preprocessed = df.filter("height >= %d and width >= %d"%filter_size)

    # detection
    model_broadcast = detector.broadcast()
    detect_udf = detector.get_detect_udf(model_broadcast)
    res = preprocessed.select("url", detect_udf(col("bytes")).alias("prediction"), "bytes")
    res.write.json(args.output_path)
