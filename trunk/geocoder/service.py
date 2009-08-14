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

"""Defines the geocoder service interface."""

__author__ = 'api.roman.public@gmail.com (Roman Nurik)'


class Error(Exception):
  pass


class GeocodeAgainError(Error):
  pass


class NotFoundError(Error):
  pass


class Result(object):
  def __init__(self, lat, lon, meta=None):
    self.lat = lat
    self.lon = lon
    self.meta = meta


class GeocoderService(object):
  def geocode_address(self, address):
    """Geocodes an address and returns a GeocodeResult if the address is found.
    Otherwise, throws an exception.
    
    Returns:
      A GeocodeResult detailing the result of the geocode.
    
    Raises:
      NotFoundError: The address wasn't found.
      GeocodeAgainError: The address could not be geocoded at this time and
          the geocode attempt should be retried.
    """
    raise NotImplementedError()
