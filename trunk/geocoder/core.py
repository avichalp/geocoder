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

"""A multi-threaded batch geocoder that uses an arbitrary input and output
stream, geocoding service, and optional record cache.
"""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'

import re
import signal
import threading
import time
import Queue

import data
import io
import service
import recordcache

NUM_THREADS = 5


class BatchGeocoder(object):
  """Instances of this class geocode data from an input data stream by querying
  a geocoder service, store them to an intermediate record cache, and write the
  results to an output data stream.
  """
  
  def __init__(self, in_stream, out_stream, service, record_cache=None,
               on_progress=None, on_complete=None):
    self._in_stream = in_stream
    self._out_stream = out_stream
    self._service = service
    self._record_cache = record_cache or recordcache.DefaultRecordCache
    self._on_progress = on_progress or (lambda: None)
    self._on_complete = on_complete or (lambda: None)
    
    # Read-time state.
    self.read_changed = 0
    self.read_new = 0
    self.read_skipped = 0
    
    # Geocode-time state.
    self._geocode_threads = []
    self._geocode_queue = Queue.Queue()
    self.geocoded_failed = 0
    self.geocoded_succeeded = 0
    self.all_geocoded = False
    
    # Write-time state.
    self._write_thread = None
    self._write_queue = Queue.Queue()
    self.written = 0
  
  def run(self):
    """Begins the batch geocoder."""
    self._read_input()
    self._start_geocoder()
    self._start_writer()
    
    # Block until threads have completed.
    block_on_threads = self._geocode_threads + [self._write_thread]
    for thread in block_on_threads:
      while thread.isAlive():
        time.sleep(1)

  def _read_input(self):
    """Reads input from the input stream and prepares the geocode queue."""
    if not self._in_stream:
      return
    
    for record in self._in_stream:
      if self._record_cache.exists(record.id):
        if self._record_cache.changed(record):
          self._geocode_queue.put(record)
          self.read_changed += 1
          self._on_progress(self)
        else:
          # Skip the record, but if the output stream wants a full update,
          # add the record to the write queue.
          if not self._out_stream.partial:
            self._write_queue.put(self._record_cache.get(record.id))
          self.read_skipped += 1
          self._on_progress(self)
      else:
        self._geocode_queue.put(record)
        self.read_new += 1
        self._on_progress(self)

  def _start_geocoder(self):
    """Begins asynchronous (non-blocking) batch geocoding on the geocode queue
    via the assigned geocoder. Changes will be written to the temp store.
    """
    for _ in range(NUM_THREADS):
      t = threading.Thread(target=self._threadfn_geocode)
      t.setDaemon(True)
      t.start()
      self._geocode_threads.append(t)
  
  def _start_writer(self):
    """Begins asynchronous (non-blocking) writing to the output stream."""
    self._write_thread = threading.Thread(target=self._threadfn_write)
    self._write_thread.setDaemon(True)
    self._write_thread.start()
  
  def _threadfn_geocode(self):
    """Worker thread function for geocoding."""
    while True:
      record = None
      
      try:
        record = self._geocode_queue.get(timeout=1)
        result = self._service.geocode_address(record.address)
        record.lat = result.lat
        record.lon = result.lon
        if record.meta and result.meta:
          record.meta.update(result.meta)
        
        record.status = data.Status.SUCCESS
        self.geocoded_succeeded += 1
        self._write_queue.put(record)
        self._geocode_queue.task_done()
        self._on_progress(self)
      except Queue.Empty:
        self.all_geocoded = True
        return
      except service.NotFoundError:
        record.status = data.Status.NOT_FOUND
        self.geocoded_failed += 1
        self._write_queue.put(record)
        self._geocode_queue.task_done()
        self._on_progress(self)
      except service.GeocodeAgainError:
        self._geocode_queue.put(record)
        self._geocode_queue.task_done()
      # TODO(romannurik): handle other possible exceptions.
      #except:
      #  record.status = data.Status.SERVER_ERROR
      #  self.geocoded_failed += 1
      #  self._write_queue.put(record)
      #  self._geocode_queue.task_done()
      #  self._on_progress(self)

  def _threadfn_write(self):
    """Worker thread function for writing records to the output stream."""
    while True:
      try:
        record = self._write_queue.get(timeout=1)
        self._record_cache.put(record, buffered=True)
        self._out_stream.put(record)
        self.written += 1
        self._on_progress(self)
      except Queue.Empty:
        if self.all_geocoded:
          self._record_cache.flush()
          self._on_complete(self)
          return
