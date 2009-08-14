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

"""Implements a basic CSV record reader/writer for batch geocoders."""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'

import csv

from geocoder.data import Record
from geocoder.io import InputRecordStream, OutputRecordStream

DEFAULT_OUTPUT_SPEC = [(field, field) for field in ['id', 'address', 'status',
                                                    'lat', 'lon']]


class SimpleObject:
  def __init__(self, **kwargs):
    for attr in kwargs:
      self.__dict__[attr] = kwargs[attr]


class CSVInputRecordStream(InputRecordStream):
  def __init__(self, filename, has_headers=False, id_col=0, address_cols=None,
               lat_lon_cols=None, status_col=None):
    """If has_headers is True, this constructor will read the first line of
    the input file to retrieve column headers.
    """
    self._filename = filename
    self._reader = csv.reader(open(filename, 'r'))
    self._id_col = id_col
    self._address_cols = address_cols or [1]
    self._lat_lon_cols = lat_lon_cols or [self._address_cols[-1] + 1,
                                          self._address_cols[-1] + 2]
    self._status_col = status_col or self._lat_lon_cols[1] + 1
    
    # State.
    self._eof = False
    
    # Read headers.
    self.col_headers = []
    if has_headers:
      try:
        self.col_headers = self._reader.next()
      except StopIteration:
        self.col_headers = []
  
  def next(self):
    try:
      row = self._reader.next()
      
      rec_dict = {}
      rec_dict['id'] = row[self._id_col]
      rec_dict['address'] = ', '.join(row[col].strip()
                                      for col in self._address_cols)
      if self._lat_lon_cols[1] < len(row):
        rec_dict['lat'] = row[self._lat_lon_cols[0]]
        rec_dict['lon'] = row[self._lat_lon_cols[1]]
      
      if self._status_col < len(row):
        rec_dict['status'] = row[self._status_col]
      
      # Store all loaded columns in the record's meta field for potential use
      # by the output stream writer, both by numerical index and column
      # header keys, if column headers are available.
      col_dict = dict((i, row[i]) for i in range(len(row)))
      if self.col_headers:
        col_dict.update(dict((hdr, row[i])
                             for i, hdr in enumerate(self.col_headers)))
      rec_dict['meta'] = dict(columns=col_dict)
      
      return Record.from_dict(rec_dict)
    except StopIteration:
      self._eof = True
      raise StopIteration()
  
  def eof(self):
    return self._eof


class CSVOutputRecordStream(OutputRecordStream):
  partial = False
  
  def __init__(self, filename, output_spec=None, write_headers=False):
    """If write_headers is True, this constructor will write the column
    headers defined in the list of (header, colspec) tuples defined in
    output_spec.
    """
    self._filename = filename
    self._writer = csv.writer(open(filename, 'w'))
    self._output_spec = output_spec or DEFAULT_OUTPUT_SPEC
    
    if write_headers:
      self._writer.writerow([col[0] for col in self._output_spec])
  
  def put(self, record):
    row = []
    
    # Define global variables available to expression fields.
    expr_globals = dict(record.__dict__.items() +
                        ([('columns', record.meta['columns'])]
                         if record.meta and 'columns' in record.meta
                         else []))
    
    for field in [col[1] for col in self._output_spec]:
      if field[0] == '=':
        # Treat the field as an expression.
        expr = field[1:]
        row.append(eval(expr, expr_globals))
      else:
        row.append(getattr(record, field))
    
    self._writer.writerow(row)
