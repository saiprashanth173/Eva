from abc import ABC
from typing import Any

from torch import nn
from torchvision.transforms import Compose, transforms

from src.models.storage.batch import FrameBatch
from src.udfs.abstract_udfs import AbstractClassifierUDF


class PytorchAbstractClassifierUDF(AbstractClassifierUDF, nn.Module, ABC):
    """
    A pytorch based classifier. Used to make sure we make maximum
    utilization of features provided by pytorch without reinventing the wheel.
    """

    def __init__(self):
        AbstractClassifierUDF.__init__(self)
        nn.Module.__init__(self)

    def get_device(self):
        return next(self.parameters()).device

    @property
    def transforms(self) -> Compose:
        return Compose([transforms.ToTensor()])

    def transform(self, images: Any):
        return self.transforms(images).to(self.get_device())

    def forward(self, batch: FrameBatch):
        return self.classify(batch)

    def __call__(self, *args, **kwargs):
        return nn.Module.__call__(self, *args, *kwargs)
