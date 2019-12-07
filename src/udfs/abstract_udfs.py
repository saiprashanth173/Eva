from abc import ABCMeta, abstractmethod
from typing import List

from src.models.catalog.frame_info import FrameInfo
from src.models.inference.base_prediction import BasePrediction
from src.models.storage.batch import FrameBatch


class AbstractClassifierUDF(metaclass=ABCMeta):
    """
    Abstract class for UDFs. All the UDFs which perform classification
    inherit this calls.

    Load and initialize the machine learning model in the __init__.

    """

    def __init__(self):
        pass

    @property
    @abstractmethod
    def input_format(self) -> FrameInfo:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def labels(self) -> List[str]:
        """
        Returns:
            List[str]: list of labels the classifier predicts
        """
        pass

    @abstractmethod
    def classify(self, batch: FrameBatch) -> List[BasePrediction]:
        """
        Takes as input a batch of frames and returns the predictions by
        applying the classification model.

        Arguments:
            batch (FrameBatch): Input batch of frames on which prediction
            needs to be made

        Returns:
            List[BasePrediction]: The predictions made by the classifier
        """
        pass

    def __call__(self, *args, **kwargs):
        return self.classify(*args, **kwargs)
