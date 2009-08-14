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

"""Implements a Google Maps API HTTP-based geocoder for use in batch
geocoders.
"""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'

import simplejson
import time
import urllib

from geocoder.service import GeocoderService, Result, Error, \
                             GeocodeAgainError, NotFoundError

MAX_GEOCODE_ACCURACY = 9
GEOCODE_URL_TEMPLATE = ('http://maps.google.com/maps/geo?q=%(address)s'
                        '&output=json&oe=utf8&sensor=false&key=%(maps_key)s')


class GoogleGeocoderStatus:
  # No errors occurred; the address was successfully parsed and its geocode
  # was returned.
  SUCCESS = 200
  
  # A geocoding or directions request could not be successfully processed, yet
  # the exact reason for the failure is unknown.
  SERVER_ERROR = 500
  
  # An empty address was specified in the HTTP q parameter.
  MISSING_QUERY = 601
  
  # No corresponding geographic location could be found for the specified
  # address, possibly because the address is relatively new, or because it may
  # be incorrect.
  UNKNOWN_ADDRESS = 602
  
  # The geocode for the given address or the route for the given directions
  # query cannot be returned due to legal or contractual reasons.
  UNAVAILABLE_ADDRESS = 603
  
  # The given key is either invalid or does not match the domain for which it
  # was given.
  BAD_KEY = 610
  
  # The given key has gone over the requests limit in the 24 hour period or
  # has submitted too many requests in too short a period of time. If you're
  # sending multiple requests in parallel or in a tight loop, use a timer or
  # pause in your code to make sure you don't send the requests too quickly.
  TOO_MANY_QUERIES = 620


class GoogleMapsService(GeocoderService):
  def __init__(self, maps_key):
    self._maps_key = maps_key
    self._backoff_seconds = 0
  
  def geocode_address(self, address):
    if self._backoff_seconds:
      time.sleep(self._backoff_seconds)
    
    geocode_url = (GEOCODE_URL_TEMPLATE %
                   dict(address=urllib.quote_plus(address),
                        maps_key=self._maps_key))

    response_file = urllib.urlopen(geocode_url)
    response_json = response_file.read()
    #try:
    response_obj = simplejson.loads(response_json, encoding='latin1')
    #except UnicodeDecodeError:
    #  print >> sys.stderr, 'Error with: %s' % result_json
    #  print sys.exc_info()
    #  return
  
    geocode_status = response_obj['Status']['code']
    if geocode_status == GoogleGeocoderStatus.SUCCESS:
      self._backoff_seconds = 0
      placemark = response_obj['Placemark'][0]
      return Result(lat=placemark['Point']['coordinates'][1],
                    lon=placemark['Point']['coordinates'][0],
                    meta=dict(accuracy=placemark['AddressDetails']['Accuracy']))
    elif geocode_status == GoogleGeocoderStatus.TOO_MANY_QUERIES:
      self._backoff_seconds = (1 if not self._backoff_seconds
                               else self._backoff_seconds * 2)
      raise GeocodeAgainError()
    elif geocode_status == GoogleGeocoderStatus.UNKNOWN_ADDRESS:
      raise NotFoundError()
    elif geocode_status == GoogleGeocoderStatus.BAD_KEY:
      raise Error('Bad Maps API key provided.')
    else:
      raise Error('Geocoder error %d.' % geocode_status)
