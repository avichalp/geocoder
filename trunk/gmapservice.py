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

DELAY_INIT = 0  # Initial delay in between geocode attempts.
DELAY_INCREMENT = 0.1  # Add 100ms to delay every time we get a 620 error.
DELAY_DECAY = 0.01  # Lower the delay by 1% for each successful geocode.


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
    self._delay = DELAY_INIT
  
  def geocode_address(self, address):
    time.sleep(self._delay)
    
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
      #self._delay = 0
      self._delay = self._delay * (1 - DELAY_DECAY)
      placemark = response_obj['Placemark'][0]
      return Result(lat=placemark['Point']['coordinates'][1],
                    lon=placemark['Point']['coordinates'][0],
                    meta=dict(accuracy=placemark['AddressDetails']['Accuracy']))
    elif geocode_status == GoogleGeocoderStatus.TOO_MANY_QUERIES:
      #self._delay = (1 if not self._delay else self._delay * 2)
      self._delay += DELAY_INCREMENT
      raise GeocodeAgainError()
    elif geocode_status == GoogleGeocoderStatus.UNKNOWN_ADDRESS:
      raise NotFoundError()
    elif geocode_status == GoogleGeocoderStatus.BAD_KEY:
      raise Error('Bad Maps API key provided.')
    else:
      raise Error('Geocoder error %d.' % geocode_status)
