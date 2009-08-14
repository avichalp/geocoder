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

"""Implements a SQLite3-based record cache."""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'

import pickle
import sqlite3
import threading

from geocoder.data import Record, Status
from geocoder.recordcache import RecordCache, DoesNotExistError

MAX_QUEUED_RECORD_PUTS = 100


class SqliteRecordCache(RecordCache):
  def __init__(self, filename):
    self._filename = filename
    self._caches_by_thread = {}
  
  def _get_current_cache(self):
    current_thread = threading.currentThread()
    try:
      return self._caches_by_thread[current_thread]
    except KeyError:
      new_cache = SingleThreadSqliteRecordCache(self._filename)
      self._caches_by_thread[current_thread] = new_cache
      return new_cache
  
  def get(self, id):
    return self._get_current_cache().get(id)
  
  def put(self, record, buffered=False):
    self._get_current_cache().put(record, buffered)
  
  def flush(self):
    self._get_current_cache().flush()


class SingleThreadSqliteRecordCache(RecordCache):
  def __init__(self, filename):
    self._filename = filename
    self._db = sqlite3.connect(filename)
    self._db.text_factory = str  # TODO(romannurik): fix for unicode support?
    self._queued_puts = 0
  
    def _dict_factory(cursor, row):
      row_dict = {}
      for i, col in enumerate(cursor.description):
        row_dict[col[0]] = row[i]
      return row_dict

    self._db.row_factory = _dict_factory
  
    self._db.execute('create table if not exists records '
                     '(id text primary key, status int, address text, '
                     'lat real, lon real, meta text)')
    self._db.commit()

  def get(self, id):
    rec_query = self._db.execute('select id, address, status, meta, '
                                 'lat, lon from records '
                                 'where id=?', (id,))
    rec_dict = rec_query.fetchone()
    self._db.commit()
    if not rec_dict:
      raise DoesNotExistError()
  
    rec_dict['meta'] = (pickle.loads(rec_dict['meta'])
                        if 'meta' in rec_dict else None)
    return Record.from_dict(rec_dict)

  def put(self, record, buffered=False):
    rec_dict = record.as_dict()
    rec_dict['meta'] = (pickle.dumps(rec_dict['meta'])
                        if 'meta' in rec_dict else None)
    if self.exists(record.id):
      self._db.execute('update records set '
                       'status=:status, address=:address, '
                       'lat=:lat, lon=:lon, meta=:meta '
                       'where id=:id',
                       rec_dict)
    else:
      self._db.execute('insert into records '
                       '(id, status, address, lat, lon, meta) '
                       'values (:id, :status, :address, :lat, :lon, :meta)',
                       rec_dict)
    
    self._queued_puts += 1
    if not buffered or self._queued_puts > MAX_QUEUED_RECORD_PUTS:
      self.flush()
  
  def flush(self):
    self._db.commit()
    self._queued_puts = 0
