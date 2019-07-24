import argparse

import numpy as np

from model.object_detection import SSDExtractor


def get_args():
    parser = argparse.ArgumentParser(description="Extracting images from model output.")
    parser.add_argument("--res_dir", help="Path of result (model output) directory.")
    parser.add_argument(
        "--output_dir", help="Path of extracted image file output directory."
    )
    parser.add_argument(
        "--threshold", type=float, help="Threshold of detection confidence scores."
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    extractor = SSDExtractor(args.res_dir, args.output_dir)
    extractor.extract_and_save(class_ids="all", threshold=args.threshold)
