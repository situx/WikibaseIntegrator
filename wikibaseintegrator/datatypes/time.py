from __future__ import annotations

import datetime
import re
from typing import Any, Dict, Optional, Union

from wikibaseintegrator.datatypes.basedatatype import BaseDataType
from wikibaseintegrator.wbi_config import config
from wikibaseintegrator.wbi_enums import WikibaseDatePrecision, WikibaseSnakType


class Time(BaseDataType):
    """
    Implements the Wikibase data type with date and time values
    """
    DTYPE = 'time'
    PTYPE = 'http://wikiba.se/ontology#Time'
    sparql_query = '''
        SELECT * WHERE {{
          ?item_id <{wb_url}/prop/{pid}> ?s .
          ?s <{wb_url}/prop/statement/{pid}> '{value}'^^xsd:dateTime .
        }}
    '''

    def __init__(self, time: Optional[str] = None, before: int = 0, after: int = 0, precision: Union[int, WikibaseDatePrecision] = WikibaseDatePrecision.DAY, timezone: int = 0,
                 calendarmodel: Optional[str] = None, wikibase_url: Optional[str] = None, **kwargs: Any):
        """
        Constructor, calls the superclass BaseDataType

        :param time: Explicit value for point in time, represented as a timestamp resembling ISO 8601. You can use the keyword 'now' to get the current UTC date.
        :param prop_nr: The property number for this claim
        :param before: explicit integer value for how many units after the given time it could be.
                       The unit is given by the precision.
        :param after: explicit integer value for how many units before the given time it could be.
                      The unit is given by the precision.
        :param precision: Precision value for dates and time as specified in the Wikibase data model
                          (https://www.wikidata.org/wiki/Special:ListDatatypes#time)
        :param timezone: The timezone which applies to the date and time as specified in the Wikibase data model
        :param calendarmodel: The calendar model used for the date. URL to the Wikibase calendar model item or the QID.
        """

        super().__init__(**kwargs)
        self.set_value(time=time, before=before, after=after, precision=precision, timezone=timezone, calendarmodel=calendarmodel, wikibase_url=wikibase_url)

    def set_value(self, time: Optional[str] = None, before: int = 0, after: int = 0, precision: Union[int, WikibaseDatePrecision] = WikibaseDatePrecision.DAY, timezone: int = 0,
                  calendarmodel: Optional[str] = None, wikibase_url: Optional[str] = None):
        calendarmodel = calendarmodel or str(config['CALENDAR_MODEL_QID'])
        wikibase_url = wikibase_url or str(config['WIKIBASE_URL'])

        if calendarmodel.startswith('Q'):
            calendarmodel = wikibase_url + '/entity/' + calendarmodel

        assert isinstance(time, str) or time is None, f"Expected str, found {type(time)} ({time})"

        if time:
            if time == "now":
                time = datetime.datetime.utcnow().strftime("+%Y-%m-%dT00:00:00Z")

            if not (time.startswith("+") or time.startswith("-")):
                time = "+" + time
            # Pattern with precision lower than day supported
            # pattern = re.compile(r'^[+-][0-9]{1,16}-(?:1[0-2]|0[1-9])-(?:3[01]|0[1-9]|[12][0-9])T(?:2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]Z$')
            pattern = re.compile(r'^[+-][0-9]{1,16}-(?:1[0-2]|0[1-9])-(?:3[01]|0[1-9]|[12][0-9])T00:00:00Z$')
            matches = pattern.match(time)
            if not matches:
                raise ValueError(f"Time value ({time}) must be a string in the following format: '+%Y-%m-%dT00:00:00Z'")

            if isinstance(precision, int):
                precision = WikibaseDatePrecision(precision)

            if precision not in WikibaseDatePrecision:
                raise ValueError("Invalid value for time precision, see https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON#time")

            self.mainsnak.datavalue = {
                'value': {
                    'time': time,
                    'before': before,
                    'after': after,
                    'precision': precision.value,
                    'timezone': timezone,
                    'calendarmodel': calendarmodel
                },
                'type': 'time'
            }

    def from_sparql_value(self, sparql_value: Dict) -> Time:
        """
        Parse data returned by a SPARQL endpoint and set the value to the object

        :param sparql_value: A SPARQL value composed of type and value
        :return:
        """
        datatype = sparql_value['datatype']
        type = sparql_value['type']
        value = sparql_value['value']

        if datatype != 'http://www.w3.org/2001/XMLSchema#dateTime':
            raise ValueError('Wrong SPARQL datatype')

        if type != 'literal':
            raise ValueError('Wrong SPARQL type')

        if value.startswith('http://www.wikidata.org/.well-known/genid/'):
            self.mainsnak.snaktype = WikibaseSnakType.UNKNOWN_VALUE
        else:
            self.set_value(time=value)

        return self

    def get_sparql_value(self, **kwargs: Any) -> str:
        return '"' + self.mainsnak.datavalue['value']['time'] + '"^^xsd:dateTime'
