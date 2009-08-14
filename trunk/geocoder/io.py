# Copyright 2009 Roman Nurik
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

"""Defines interfaces for input and output data record streams."""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'

import csv
import pickle

from data import Record, Status


class InputRecordStream(object):
  """Defines an abstract interface for input sources that can provide input
  records to the geocoder.
  """
  
  def __iter__(self):
    return self
  
  def next(self):
    """Returns the next record in the input stream.
    
    Returns:
      The next geocoder.data.Record in the stream.
    
    Raises:
      StopIteration: No more records were available.
    """
    raise NotImplementedError()
  
  def eof(self):
    """Returns True if all records have been read."""
    raise NotImplementedError()

  
class OutputRecordStream(object):
  """Defines an abstract interface for output sources that can accept output
  records to the geocoder.
  """
  
  partial = False
  
  def put(self, rec):
    """Writes a record to the output stream.
    
    Args:
      rec: A geocoder.data.Record to write to the output stream.
    """
    raise NotImplementedError()
