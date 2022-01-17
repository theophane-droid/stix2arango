from stix2 import IPv4Address, IPv6Address
import ipaddress
import socket
import struct

from stix2arango.exceptions import FieldCanNotBeCalculatedBy

def int_from_ipv6(addr):
    hi, lo = struct.unpack('!QQ', socket.inet_pton(socket.AF_INET6, addr))
    return (hi << 64) | lo

# overwride stix core object
class IPV4Modifier(IPv4Address):
    type = 'ipv4-addr' # this specifify that we will overwrite all the 'ipv4-addr' objects
    def __init__(self, **kwargs):
        if '/' in kwargs['value'] : # means cidr
            extra = self.calc_ip_range(kwargs['value'])
        else :
            extra = self.calc_ip_int(kwargs['value'])
        super().__init__(**kwargs, custom_properties=extra)

    def calc_ip_range(self, ip):
        net = ipaddress.ip_network(ip)
        range = {'network_addr': int(net.network_address),
                'broadcast_addr': int(net.broadcast_address)
                }
        return {'x_ip' : range}

    def calc_ip_int(self, ip):
        ip = ipaddress.ip_address(ip)
        return {'x_ip' : int(ip)}

    def eval(field, operator, value):
        if field ==  'ipv4-addr:x_ip':
            ip = ipaddress.ip_address(value[1:-1])
            return """f.x_ip == {}
            OR (f.x_ip.network_addr <= {} AND f.x_ip.broadcast_addr >= {})
            """.format(int(ip), int(ip), int(ip))
        else:
            raise FieldCanNotBeCalculatedBy(field, type)