import logging

import pytest
import requests

from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.datatypes import BaseDataType, Item
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator.wbi_exceptions import NonExistentEntityError

wbi_config['USER_AGENT'] = 'WikibaseIntegrator-pytest/1.0 (test_entity_item.py)'
wbi_config['MEDIAWIKI_API_URL'] = 'http://localhost/w/api.php'
wbi_config['SPARQL_ENDPOINT_URL'] = 'http://localhost:8834/proxy/wdqs/bigdata/namespace/wdq/sparql'
wbi_config['WIKIBASE_URL'] = 'http://wikibase.svc'

wbi = WikibaseIntegrator(login=wbi_login.Login(user='admin', password='change-this-password'))
logging.basicConfig(level=logging.DEBUG)


@pytest.mark.order('first')
@pytest.fixture
def test_item_creation():
    return wbi.item.new().write().id


def test_get(test_item_creation):
    entity_id = test_item_creation
    # Test with complete id
    assert wbi.item.get(entity_id).id == entity_id
    # Test with numeric id as string
    assert wbi.item.get(entity_id).id == entity_id
    # Test with numeric id as int
    assert wbi.item.get(entity_id).id == entity_id

    # Test with invalid id
    with self.assertRaises(ValueError):
        wbi.item.get('L5')

    # Test with zero id
    with self.assertRaises(ValueError):
        wbi.item.get(0)

    # Test with negative id
    with self.assertRaises(ValueError):
        wbi.item.get(-1)

    # Test with negative id
    with self.assertRaises(NonExistentEntityError):
        wbi.item.get("Q99999999999999")


def test_get_json():
    assert wbi.item.get('Q582').get_json()['labels']['fr']['value'] == 'Villeurbanne'


def test_write():
    with self.assertRaises(requests.exceptions.JSONDecodeError):
        wbi.item.get('Q582').write(allow_anonymous=True, mediawiki_api_url='https://httpstat.us/200')


def test_write_not_required():
    assert not wbi.item.get('Q582').write_required(base_filter=[BaseDataType(prop_nr='P1791')])


def test_write_required():
    item = wbi.item.get('Q582')
    item.claims.add(Item(prop_nr='P1791', value='Q42'))
    assert item.write_required([BaseDataType(prop_nr='P1791')])


def test_write_not_required_ref():
    assert not wbi.item.get('Q582').write_required(base_filter=[BaseDataType(prop_nr='P2581')], use_refs=True)


def test_write_required_ref():
    item = wbi.item.get('Q582')
    item.claims.get('P2581')[0].references.references.pop()
    assert item.write_required(base_filter=[BaseDataType(prop_nr='P2581')], use_refs=True)


def test_long_item_id():
    assert wbi.item.get('Item:Q582').id == 'Q582'
