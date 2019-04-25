import pickle
import os
import json
import numpy as np
from .preprocess import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, col
from pyspark.sql.types import ArrayType, FloatType
import tensorflow as tf
import pandas as pd


PKG_DIR = os.path.dirname(__file__)


class ImageExtractor:
    def __init__(self, res_dir, output_dir):
        self.res_dir = res_dir
        self.output_dir = output_dir


    def _extract_and_save(self, rec, class_ids, threshold):
        raise NotImplementedError("Please overwrite this method.")


    def extract_and_save(self, class_ids, threshold):
        if class_ids == "all":
            class_ids = list(self.cate_dict.keys())

        for idx in class_ids:
            cls = self.cate_dict[idx]
            check_dir(self.output_dir + "/%s/"%cls, create=True)

        for fname in os.listdir(self.res_dir):
            if fname.startswith("part-"):
                print("Extracting:", self.res_dir+"/"+fname)
                with open(self.res_dir+"/"+fname) as f:
                    for line in f:
                        rec = json.loads(line)
                        self._extract_and_save(rec, class_ids, threshold)


class SSD:
    def __init__(self, sc, sql_context, args):
        self.sc = sc
        self.sql_context = sql_context
        self.category = pickle.load(open("%s/category/mscoco.pickle"%PKG_DIR, "rb"))
        self.checkpoint = "%s/graph/ssd_mobilenet_v1_fpn_640x640/frozen_inference_graph.pb"%PKG_DIR
        self.args = args
        with tf.gfile.FastGFile(self.checkpoint, 'rb') as f:
            model_params = f.read()
        self.model_params = model_params


    def broadcast(self):
        return self.sc.broadcast(self.model_params)


    def get_detect_udf(self, model_broadcast):
        def batch_proc(bytes_batch):
            with tf.Graph().as_default() as g:
                graph_def = tf.GraphDef()
                graph_def.ParseFromString(model_broadcast.value)
                tf.import_graph_def(graph_def, name='')
                image_tensor = g.get_tensor_by_name('image_tensor:0')
                detection_scores = g.get_tensor_by_name('detection_scores:0')
                detection_classes = g.get_tensor_by_name('detection_classes:0')

                with tf.Session().as_default() as sess:
                    result = []
                    image_size = (640, 640)
                    images = np.array([img2np(b, image_size) for b in bytes_batch])
                    res = sess.run([detection_scores, detection_classes], feed_dict={image_tensor: images})
                    for i in range(res[0].shape[0]):
                        result.append([res[0][i], res[1][i]])
            return pd.Series(result)
        return pandas_udf(ArrayType(ArrayType(FloatType())), PandasUDFType.SCALAR)(batch_proc)


class SSDExtractor(ImageExtractor):
    def __init__(self, res_dir, output_dir):
        super().__init__(res_dir, output_dir)
        self.cate_dict = pickle.load(open("%s/category/mscoco.pickle"%PKG_DIR, "rb"))


    def _extract_and_save(self, rec, class_ids, threshold):
        pred = rec['prediction']
        scores = np.array(pred[0])
        classes = np.array(pred[1])
        valid_classes = np.unique(classes[scores >= threshold])
        if valid_classes.shape[0] > 0:
            if class_ids != "all":
                inter = list(set(valid_classes).intersection(set(class_ids)))
                if len(inter) > 0:
                    valid_classes = np.array(inter)
                else:
                    valid_classes = None
        else:
            valid_classes = None

        if valid_classes is not None:
            for cls_idx in valid_classes:
                cls = self.cate_dict[cls_idx]
                try:
                    img = str2img(rec["bytes"])
                    img.save(self.output_dir+ "/%s/"%cls + url_parse(rec["url"]))
                except:
                    fname = self.output_dir+ "/%s/"%cls + url_parse(rec["url"])
                    print("Failing to save:", fname)


