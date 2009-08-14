#!/usr/bin/env python
#
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

"""A batch geocoder example application that reads record data from a CSV file,
geocodes the specified address columns using the Google Maps API geocoder
service, and writes results to a new CSV file with a given output column
specification.

Run this script with no arguments for usage and more help.
"""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'

import optparse
import re
import signal
import sys
import threading

from csvio import CSVInputRecordStream, CSVOutputRecordStream
from geocoder.core import BatchGeocoder
from gmapservice import GoogleMapsService
from sqliterecordcache import SqliteRecordCache


def parse_colranges(colranges):
  cols = []
  for indspec in colranges.split(','):
    range_match = re.match(r'(\d+)-(\d+)', indspec)
    if range_match:
      for c in range(int(range_match.group(1)) - 1, int(range_match.group(2))):
        cols.append(c)
    else:
      try:
        cols.append(int(indspec) - 1)
      except ValueError:
        cols.append(indspec)
  
  return cols


def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] '
                                       'inputcsvfile outputcsvfile')
  parser.add_option('-d', '--headers',
                    action='store_true',
                    dest='has_headers',
                    help='the input CSV file has headers')
  parser.add_option('-t', '--temp-file',
                    dest='temp_db_file',
                    metavar='FILE',
                    default='*DEF*',
                    help='temporary SQLite3 state/storage file')
  parser.add_option('-i', '--id-col',
                    dest='id_col',
                    metavar='COL',
                    default='1',
                    help='1-based index of the column containing a unique ID '
                         'identifying the record')
  parser.add_option('-a', '--address-cols',
                    dest='address_cols',
                    metavar='COLS',
                    help='1-based, comma-separated indices of the columns '
                         'constituting the address of the record')
  parser.add_option('-c', '--output-cols',
                    dest='output_cols',
                    metavar='COLS',
                    help="1-based, comma-separated indices of the columns "
                         "from the original CSV to output, along with 'id', "
                         "'lat', 'lon', and 'accuracy'")
  parser.add_option('-k', '--maps-key',
                    dest='maps_key',
                    help='Google Maps API key')
  (options, args) = parser.parse_args()
  if len(args) != 2:
    parser.print_help()
    raise SystemExit
  
  if not options.address_cols:
    print >> sys.stderr, '-a/--address-cols is required'
    raise SystemExit
  
  if not options.output_cols:
    print >> sys.stderr, '-c/--output-cols is required'
    raise SystemExit
  
  if not options.maps_key:
    print >> sys.stderr, '-k/--maps-key is required'
    raise SystemExit
  
  if options.temp_db_file == '*DEF*':
    options.temp_db_file = '.tmpdb.%s.sql3' % args[0]

  def _on_progress(geocoder):
    print ("Read: (%d new, %d change, %d skip), "
           "Geocode: (%d good, %d bad), "
           "Write: %d" %
           (geocoder.read_new, geocoder.read_changed, geocoder.read_skipped,
            geocoder.geocoded_succeeded, geocoder.geocoded_failed,
            geocoder.written)), '\r',
    sys.stdout.flush()
  
  def _on_complete(geocoder):
    print
    print 'Geocode complete.'
  
  try:
    in_stream = CSVInputRecordStream(
        args[0],
        has_headers=options.has_headers,
        address_cols=parse_colranges(options.address_cols))
    
    output_spec = []
    for col in parse_colranges(options.output_cols):
      if isinstance(col, str):
        if col == 'accuracy':
          output_spec.append((col, '=meta["accuracy"] if "accuracy" in meta '
                                   'else None'))
        elif col in ['id', 'lat', 'lon']:
          output_spec.append((col, col))
      else:
        if in_stream.col_headers:
          output_spec.append((in_stream.col_headers[col], '=columns[%d]' % col))
        else:
          output_spec.append((str(col), '=columns[%d]' % col))

    out_stream = CSVOutputRecordStream(args[1],
        write_headers=options.has_headers,
        output_spec=output_spec)
    
    geocoder = BatchGeocoder(
        in_stream=in_stream,
        out_stream=out_stream,
        service=GoogleMapsService(maps_key=options.maps_key),
        record_cache=SqliteRecordCache(options.temp_db_file),
        on_progress=_on_progress,
        on_complete=_on_complete)
    geocoder.run()
  except KeyboardInterrupt:
    pass

if __name__ == '__main__':
  main()
