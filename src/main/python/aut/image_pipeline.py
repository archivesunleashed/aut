import numpy as np
import tensorflow as tf
from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

def run_inference_on_image(sess, image_data, node_lookup):
	"""Download an image, and run inference on it.

	Args:
	image_data: Image bytes

	Returns:
	  scores: a list of (human-readable node names, score) pairs
	"""
	  # Some useful tensors:
	  # 'softmax:0': A tensor containing the normalized prediction across
	  #   1000 labels.
	  # 'pool_3:0': A tensor containing the next-to-last layer containing 2048
	  #   float description of the image.
	  # 'DecodeJpeg/contents:0': A tensor containing a string providing JPEG
	  #   encoding of the image.
	  # Runs the softmax tensor by feeding the image_data as input to the graph.
	softmax_tensor = sess.graph.get_tensor_by_name('softmax:0')
	try:
	  	predictions = sess.run(softmax_tensor,
	                           {'DecodeJpeg/contents:0': image_data})
	except:
	    # Handle problems with malformed JPEG files
	    return (img_id, img_url, None)
	predictions = np.squeeze(predictions)
	top_k = predictions.argsort()[-num_top_predictions:][::-1]
	scores = []
	for node_id in top_k:
	    if node_id not in node_lookup:
	      	human_string = ''
	    else:
	      	human_string = node_lookup[node_id]
	   	score = predictions[node_id]
	    scores.append((human_string, score))
	return scores

def apply_inference_on_batch(image_bytes):
	"""Apply inference to a batch of images.
		We do not explicitly tell TensorFlow to use a GPU.
		It is able to choose between CPU and GPU based on its guess of which will be faster.
  	"""
	with tf.Graph().as_default() as g:
		graph_def = tf.GraphDef()
		graph_def.ParseFromString(model_data_bc.value)
		tf.import_graph_def(graph_def, name='')
	with tf.Session() as sess:
		return run_inference_on_image(sess, image_bytes, node_lookup_bc.value)

#labeled_images = images.flatMap(apply_inference_on_batch)

