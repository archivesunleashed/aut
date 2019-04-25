from PIL import Image
import io
import base64
import os
import numpy as np

def str2img(byte_str):
    return Image.open(io.BytesIO(base64.b64decode(bytes(byte_str, 'utf-8'))))


def img2np(byte_str, resize=None):
    try:
        image = str2img(byte_str)
        img = image.convert("RGB")
        if resize is not None:
            img = img.resize(resize, Image.BILINEAR)
        img = np.array(img).astype(np.uint8)
        img_shape = np.shape(img)

        if len(img_shape) == 2:
            img = np.stack([img, img, img], axis=-1)
        elif img_shape[-1] >= 3:
            img = img[:,:,:3]

        return img

    except:
        if resize is not None:
            return np.zeros((resize[0], resize[1], 3))
        else:
            return np.zeros((1, 1, 3))


def url_parse(url):
    return url.split("://")[1].replace("/", "%%%%")


def check_dir(path, create=False):
    if os.path.exists(path):
        return True
    else:
        if create:
            os.makedirs(path, exist_ok=True)
        return False


