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

"""Defines common data objects used throughout the batch geocoder library."""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'

import md5
import pickle

RECORD_FIELDS = ['id', 'status', 'address', 'lat', 'lon', 'meta']


class Status(object):
  SUCCESS = 1
  NOT_FOUND = -1
  SERVER_ERROR = -2


class Record(object):
  """Instances of this class represent a single address record that may or may
  not be geocoded.
  """
  
  def __init__(self, **kwargs):
    # Initialize record data fields to None.
    for field in RECORD_FIELDS:
      setattr(self, field, None)
    
    for kw in kwargs:
      setattr(self, kw, kwargs[kw])

  def __hash__(self):
    return md5.md5(pickle.dumps((id, address, status, lat, lon))).hexdigest()
  
  def as_dict(self):
    return dict((field, getattr(self, field) if hasattr(self, field) else None)
                for field in RECORD_FIELDS)
  
  @staticmethod
  def from_dict(dict_obj):
    return Record(**dict_obj)
