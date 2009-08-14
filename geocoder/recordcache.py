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

"""Defines an interface for record caches, which are designed to prevent
already-geocoded data records from being geocoded again unless they are
changed.
"""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'


class Error(Exception):
  pass


class DoesNotExistError(Error):
  pass


class RecordCache(object):
  def get(id):
    """Returns the record with the given ID from the record cache.
    
    Returns:
      A geocoder.data.Record with the given ID.
    
    Raises:
      A geocoder.recordcache.DoesNotExistError if no such record exists.
    """
    raise NotImplementedError()
  
  def put(self, record, buffered=False):
    """Puts the record to the record cache. When called with buffered=True
    on record caches that support buffering, queues to maximize throughput.
    Note that at the end of a sequence of buffered puts, the cache must manually
    be flush via flush().
    """
    raise NotImplementedError()
  
  def flush(self):
    """Flushes any remaining buffered put operations."""
    raise NotImplementedError()
  
  def exists(self, id):
    """Returns True if a record with the given ID exists in the record cache."""
    try:
      rec = self.get(id)
      return True
    except DoesNotExistError:
      return False
    
  def changed(self, record):
    """Returns True if the given record is either new or has changed since its
    last put to the record cache."""
    try:
      current_record = self.get(record.id)
      return current_record == record
    except DoesNotExistError:
      return True


class DefaultRecordCache(RecordCache):
  """A simple, in-memory implementation of a record cache."""
  
  def __init__(self):
    self._records = {}
  
  def get(self, id):
    try:
      return self._records[id]
    except KeyError:
      raise DoesNotExistError()
      
  def put(self, record, buffered=False):
    self._records[id] = record
  
  def flush(self):
    pass
