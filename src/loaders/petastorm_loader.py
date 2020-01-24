from petastorm import make_reader

from src.models.catalog.frame_info import FrameInfo
from src.models.catalog.properties import ColorSpace
from src.models.catalog.video_info import VideoMetaInfo
from src.models.storage.batch import FrameBatch
from src.models.storage.frame import Frame


class PetastormLoader:
    def __init__(self, video_metadata: VideoMetaInfo, batch_size=1,
                 skip_frames=0, offset=None, limit=None):
        """
         Abstract class for defining video loader.
         All other video loaders use this abstract class.
         Video loader are expected fetch the videos from storage
         and return the frames in an iterative manner.

         Attributes:
             video_metadata (VideoMetaInfo):
             Object containing metadata of the video
             batch_size (int, optional):
            No. of frames to fetch in batch from video
             skip_frames (int, optional):
             Number of frames to be skipped while fetching the video
             offset (int, optional): Start frame location in video
             limit (int, optional): Number of frames needed from the video
         """
        self.video_metadata = video_metadata
        self.batch_size = batch_size
        self.skip_frames = skip_frames
        self.offset = offset
        self.limit = limit

    def load(self):
        info = None
        with make_reader(self.video_metadata.file) as reader:
            frames = []
            for frame_ind, row in enumerate(reader):
                if info is None:
                    (height, width, num_channels) = row.frame_data.shape
                    info = FrameInfo(height, width, num_channels,
                                     ColorSpace.BGR)

                eva_frame = Frame(row.frame_id, row.frame_data, info)

                frames.append(eva_frame)
                if self.limit and frame_ind > self.limit:
                    return FrameBatch(frames, info)

                if len(frames) % self.batch_size == 0:
                    yield FrameBatch(frames, info)
                    frames = []

            if frames:
                return FrameBatch(frames, info)
