import unittest

import sys
from datetime import datetime

sys.path.insert(0, '/app')

from stix2 import DomainName, Identity, Relationship, IPv4Address, ThreatActor

from stix2arango.utils import update_id_for_sdo, update_uid_for_obj_list, merge_obj_list

class TestUpdateIDForSDO(unittest.TestCase):
    def setUp(self):      
        sco0 = IPv4Address(value='8.8.8.8')
        sco1 = DomainName(value='google.fr', resolves_to_refs=sco0)
        sco2 = DomainName(value='google.fr', custom_properties={'x_coucou':1})
        sco3 = DomainName(value='google.com')
        sco4 = IPv4Address(value='8.8.8.8')

        sdo0 = ThreatActor(name='test1')
        sdo1 = ThreatActor(name='test1')
        sdo2 = ThreatActor(name='test2')

        self.rel = Relationship(target_ref=sco1, source_ref=sco2, relationship_type='test')
        
        sco0 = update_id_for_sdo(sco0)
        sco1 = update_id_for_sdo(sco1)
        sco2 = update_id_for_sdo(sco2)
        sco3 = update_id_for_sdo(sco3)
        sco4 = update_id_for_sdo(sco4)
        sdo0 = update_id_for_sdo(sdo0)
        sdo1 = update_id_for_sdo(sdo1)
        sdo2 = update_id_for_sdo(sdo2)
        self.sco1 = sco1
        self.sco2 = sco2
        self.sco3 = sco3
        self.sdo1 = sdo1
        self.sdo2 = sdo2
        self.sco4 = sco4
        self.sdo0 = sdo0
        self.sdo1 = sdo1
        self.sdo2 = sdo2

    def test_update_id_for_sdo(self):        
        with self.assertRaises(TypeError):
            update_id_for_sdo(self.rel)

    def test_check_results(self):
        self.assertEqual(self.sco1.id, self.sco2.id)
        self.assertNotEqual(self.sco1.id, self.sco3.id)
        self.assertEqual(self.sdo0.id, self.sdo1.id)
        self.assertNotEqual(self.sdo0.id, self.sdo2.id)



class TestUpdateIDForManySDO(unittest.TestCase):
    def setUp(self):
        sco0 = IPv4Address(value='8.8.8.8')
        sco4 = IPv4Address(value='8.8.8.8')
        self.sco4 = update_id_for_sdo(sco4)
        sco1 = DomainName(value='google.fr', resolves_to_refs=[sco0, self.sco4.id])
        sro0 = Relationship(source_ref=sco0, target_ref=sco1, relationship_type='test')
        self.initial_id_sco0 = sco0.id
        self.sco0, self.sco1, self.sro0 = update_uid_for_obj_list([sco0, sco1, sro0])
        

    def test_update_many(self):
        sco0, sco1, sro0 = update_uid_for_obj_list([self.sco0, self.sco1, self.sro0])
        self.assertNotEqual(self.initial_id_sco0, sco0.id)
        self.assertEqual(self.sco1.resolves_to_refs[0], sco0.id)
        self.assertEqual(sco1.resolves_to_refs[1], self.sco4.id)
        self.assertEqual(sro0.source_ref, sco0.id)
        self.assertEqual(sro0.target_ref, sco1.id)


class TestMergeObj(unittest.TestCase):
    def setUp(self):
        sco0 = DomainName(value='bing.com')
        sco1 = DomainName(value='google.fr')
        obj1 = {
            'id' : sco0.id,
            'type' : 'ipv4-addr',
            'x_test' : 'a'
        }
        obj2 = {
            'id' : sco0.id,
            'type' : 'ipv4-addr',
            'x_test2' : 'b'
        }
        obj3 = {
            'id': sco1.id,
            'type' : 'domain-name',
            'value' : 'google.fr'
        }
        obj4 = {
            'id' : sco0.id,
            'type' : 'ipv4-addr',
            'x_test2' : 'a'
        }
        obj5 = {
            'id': sco1.id,
            'type' : 'domain-name',
            'value' : 'google.fr'
        }
        self.list = [obj1, obj2, obj3, obj4, obj5]
        
    def test_merge(self):
        merge_obj_list(self.list)
        self.assertEqual( len(self.list), 3)