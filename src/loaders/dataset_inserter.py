import sys

import cv2
import numpy as np
from petastorm.codecs import ScalarCodec, CompressedNdarrayCodec
from petastorm.etl.dataset_metadata import materialize_dataset
from petastorm.unischema import Unischema, UnischemaField, dict_to_spark_row
from pyspark.sql.types import IntegerType

from src.models.catalog.frame_info import FrameInfo
from src.models.catalog.properties import ColorSpace
from src.spark.session import Session
from src.utils.logging_manager import LoggingManager, LoggingLevel


def store_video_as_petastorm_dataset(video_path, dataset_name,
                                     output_file_url):
    """
    Used for inserting the video as a peta
    :param video_path:
    :param output_file_url:
    :return:
    """
    video = cv2.VideoCapture(video_path)
    session = Session()
    sc = session.get_context()
    spark = session.get_session()

    LoggingManager().log("Loading frames", LoggingLevel.CRITICAL)

    _, frame = video.read()
    if frame is not None:
        (height, width, num_channels) = frame.shape
        frame_metadata = FrameInfo(height, width, num_channels, ColorSpace.BGR)
    else:
        return

    H = frame_metadata.height
    W = frame_metadata.width
    C = frame_metadata.num_channels

    # The schema defines how the dataset schema looks like
    dataset_schema = Unischema(dataset_name, [
        UnischemaField('frame_id', np.int32, (),
                       ScalarCodec(IntegerType()), False),
        UnischemaField('frame_data', np.uint8, (H, W, C),
                       CompressedNdarrayCodec(), False),
    ])

    frames = []
    frame_id = 0
    while frame is not None:
        frame_id += 1
        frames.append({"frame_id": frame_id, "frame_data": frame})
        _, frame = video.read()

    with materialize_dataset(spark,
                             output_file_url,
                             dataset_schema):

        rows_rdd = sc.parallelize(frames).map(
            lambda x: dict_to_spark_row(dataset_schema, x)
        )

        spark.createDataFrame(rows_rdd,
                              dataset_schema.as_spark_schema()) \
            .coalesce(10) \
            .write \
            .mode('overwrite') \
            .parquet(output_file_url)


if __name__ == "__main__":
    store_video_as_petastorm_dataset(sys.argv[1], sys.argv[2], sys.argv[3])
