import sys
from datetime import datetime

sys.path.insert(0, '/app')

from stix2 import DomainName, Identity, Relationship, IPv4Address, ThreatActor

from stix2arango.utils import update_id_for_sdo, update_uid_for_obj_list, merge_obj_list

print('\n\n> Update id for SDO')

sco0 = IPv4Address(value='8.8.8.8')
sco1 = DomainName(value='google.fr', resolves_to_refs=sco0)
sco2 = DomainName(value='google.fr', custom_properties={'x_coucou':1})
sco3 = DomainName(value='google.com')
sco4 = IPv4Address(value='8.8.8.8')

sdo0 = ThreatActor(name='test1')
sdo1 = ThreatActor(name='test1')
sdo2 = ThreatActor(name='test2')

rel = Relationship(target_ref=sco1, source_ref=sco2, relationship_type='test')


sco0 = update_id_for_sdo(sco0)
sco1 = update_id_for_sdo(sco1)
sco2 = update_id_for_sdo(sco2)
sco3 = update_id_for_sdo(sco3)
sco4 = update_id_for_sdo(sco4)
sdo0 = update_id_for_sdo(sdo0)
sdo1 = update_id_for_sdo(sdo1)
sdo2 = update_id_for_sdo(sdo2)

try:
    update_id_for_sdo(rel)
    raise RuntimeError('an exception was wanted !')
except TypeError:
    pass

assert(sco1.id == sco2.id)
assert(sco1.id != sco3.id)
assert(sdo0.id == sdo1.id)
assert(sdo0.id != sdo2.id)

print('OK')


print('\n\n> Update id for many sdo')
sco0 = IPv4Address(value='8.8.8.8')
sco1 = DomainName(value='google.fr', resolves_to_refs=[sco0, sco4.id])
sro0 = Relationship(source_ref=sco0, target_ref=sco1, relationship_type='test')
initial_id_sco0 = sco0.id

sco0, sco1, sro0 = update_uid_for_obj_list([sco0, sco1, sro0])

assert(initial_id_sco0 != sco0.id)
assert(sco1.resolves_to_refs[0] == sco0.id)
assert(sco1.resolves_to_refs[1] == sco4.id)
assert(sro0.source_ref == sco0.id)
assert(sro0.target_ref == sco1.id)
print('OK')

print('\n\n> Merge obj')
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
list_ = [obj1, obj2, obj3, obj4, obj5]
merge_obj_list(list_)
assert( len(list_) == 3)
print('OK')