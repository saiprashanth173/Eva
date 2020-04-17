# coding=utf-8
# Copyright 2018-2020 EVA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest

import numpy as np

from src.models.storage.batch import FrameBatch
from test.util import create_dataframe_same, create_dataframe

NUM_FRAMES = 10


class FrameBatchTest(unittest.TestCase):

    def test_set_outcomes_method_should_set_the_predictions_with_udf_name(
            self):
        batch = FrameBatch(frames=create_dataframe(), info=None)
        batch.set_outcomes('test', [None])
        self.assertEqual([None], batch.get_outcomes_for('test'))

    def test_get_outcome_from_non_existing_udf_name_returns_empty_list(self):
        batch = FrameBatch(frames=create_dataframe(), info=None)
        self.assertEqual([], batch.get_outcomes_for('test'))

    def test_frames_as_numpy_array_should_frames_as_numpy_array(self):
        batch = FrameBatch(
            frames=create_dataframe_same(2),
            info=None)
        expected = list(np.ones((2, 1, 1)))
        actual = list(batch.frames_as_numpy_array())
        self.assertEqual(expected, actual)

    def test_return_only_frames_specified_in_the_indices(self):
        batch = FrameBatch(
            frames=create_dataframe(2),
            info=None)
        expected = FrameBatch(frames=create_dataframe(),
                              info=None)
        output = batch[[0]]
        self.assertEqual(expected, output)

    def test_fetching_frames_by_index_should_also_return_outcomes(self):
        batch = FrameBatch(
            frames=create_dataframe_same(2),
            info=None,
            outcomes={'test': [[None], [None]]})
        expected = FrameBatch(frames=create_dataframe(),
                              info=None,
                              outcomes={'test': [[None]]})
        self.assertEqual(expected, batch[[0]])

    def test_slicing_on_batched_should_return_new_batch_frame(self):
        batch = FrameBatch(
            frames=create_dataframe(2),
            info=None,
            outcomes={'test': [[None], [None]]})
        expected = FrameBatch(frames=create_dataframe(),
                              info=None,
                              outcomes={'test': [[None]]})
        self.assertEqual(batch, batch[:])
        self.assertEqual(expected, batch[:-1])

    def test_slicing_should_word_for_negative_stop_value(self):
        batch = FrameBatch(
            frames=create_dataframe(2),
            info=None,
            outcomes={'test': [[None], [None]]})
        expected = FrameBatch(frames=create_dataframe(),
                              info=None,
                              outcomes={'test': [[None]]})
        self.assertEqual(expected, batch[:-1])

    def test_slicing_should_work_with_skip_value(self):
        batch = FrameBatch(
            frames=create_dataframe(3), info=None,
            outcomes={'test': [[None], [None], [None]]})
        expected = FrameBatch(
            frames=create_dataframe(3).iloc[[0, 2], :],
            info=None,
            outcomes={'test': [[None], [None]]})
        self.assertEqual(expected, batch[::2])

    def test_fetching_frames_by_index_should_also_return_temp_outcomes(self):
        batch = FrameBatch(
            frames=create_dataframe_same(2),
            info=None,
            outcomes={'test': [[1], [2]]},
            temp_outcomes={'test2': [[3], [4]]})
        expected = FrameBatch(frames=create_dataframe(),
                              info=None,
                              outcomes={'test': [[1]]},
                              temp_outcomes={'test2': [[3]]})
        self.assertEqual(expected, batch[[0]])

    def test_set_outcomes_method_should_set_temp_outcome_when_bool_is_true(
            self):
        batch = FrameBatch(frames=create_dataframe(), info=None)
        batch.set_outcomes('test', [1], is_temp=True)
        expected = FrameBatch(frames=create_dataframe(),
                              info=None, temp_outcomes={'test': [1]})
        self.assertEqual(expected, batch)

    def test_has_outcomes_returns_false_if_the_given_name_not_in_outcomes(
            self):
        batch = FrameBatch(frames=create_dataframe(), info=None)

        self.assertFalse(batch.has_outcome('temp'))

    def test_has_outcomes_returns_true_if_the_given_name_is_in_outcomes(
            self):
        batch = FrameBatch(frames=create_dataframe(), info=None)
        batch.set_outcomes('test_temp', [1], is_temp=True)
        batch.set_outcomes('test', [1])

        self.assertTrue(batch.has_outcome('test'))
        self.assertTrue(batch.has_outcome('test_temp'))
