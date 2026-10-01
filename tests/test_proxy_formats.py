import base64
import json
import unittest

from proxy_formats import parse_proxies, public_address


def b64(value):
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip('=')


def proxies(text, hints=()):
    return parse_proxies(text.encode(), hints)


class ProxyFormatTests(unittest.TestCase):
    def test_plain_duplicates_and_unknown_protocol(self):
        r = proxies('8.8.8.8:8080\n8.8.8.8:8080\n1.1.1.1:3128\n')
        self.assertEqual(r.entries_found, 3)
        self.assertEqual(len(r.proxies), 2)
        self.assertEqual({p.protocol for p in r.proxies.values()}, {'unknown'})

    def test_single_protocol_hint_and_ambiguous_hint(self):
        self.assertEqual(next(iter(proxies('8.8.8.8:80', ['socks5']).proxies.values())).protocol, 'socks5')
        self.assertEqual(next(iter(proxies('8.8.8.8:80', ['http','socks5']).proxies.values())).protocol, 'unknown')

    def test_ipv6_and_invalid_addresses(self):
        r = proxies('[2606:4700:4700::1111]:1080\n127.0.0.1:80\n10.0.0.1:80\n999.1.1.1:80\n8.8.8.8:65536\n')
        self.assertEqual(len(r.proxies), 1)
        self.assertEqual(sum(r.invalid.values()), 4)
        self.assertIsNone(public_address('metadata.google.internal'))
        self.assertIsNone(public_address('example.com'))

    def test_authentication_is_part_of_identity(self):
        r = proxies('http://a:p%40ss@8.8.8.8:80\nhttp://a:p%40ss@8.8.8.8:80#label\nhttp://a:other@8.8.8.8:80')
        self.assertEqual(r.entries_found, 3)
        self.assertEqual(len(r.proxies), 2)
        self.assertIn({'username':'a','password':'p@ss'}, [p.settings for p in r.proxies.values()])

    def test_download_and_homepage_urls_are_not_proxies(self):
        r = proxies('https://github.com/proxifly/free-proxy-list\nhttps://8.8.8.8:443/list.txt\nhttps://youtube.com')
        self.assertEqual(len(r.proxies), 0)

    def test_csv_preserves_protocol_and_credentials(self):
        r = proxies('ip,port,protocols,username,password,country\n8.8.8.8,8080,"http,socks5",alice,secret,US\n')
        self.assertEqual({p.protocol for p in r.proxies.values()}, {'http','socks5'})
        self.assertTrue(all(p.settings == {'username':'alice','password':'secret'} for p in r.proxies.values()))

    def test_json_api_nested_lists(self):
        r = proxies(json.dumps({'data':[{'ip':'8.8.8.8','port':'80','protocols':['http','https'],'latency':25}]}))
        self.assertEqual(len(r.proxies), 2)
        self.assertTrue(all(p.settings == {} for p in r.proxies.values()))

    def test_json_lines(self):
        r = proxies('{"host":"8.8.8.8","port":8080,"type":"http"}\n{"host":"1.1.1.1","port":1080,"type":"socks5"}')
        self.assertEqual(len(r.proxies), 2)

    def test_json_combined_host_and_port(self):
        r = proxies('{"proxy_list":[{"host":"8.8.8.8:8080","country":"US"}]}')
        self.assertEqual(len(r.proxies), 1)
        self.assertEqual(next(iter(r.proxies.values())).address, '8.8.8.8')

    def test_json_endpoint_keys_and_complete_connection_strings(self):
        keyed = proxies('{"8.8.8.8:8080":{"type":"http","latency":2}}')
        self.assertEqual(next(iter(keyed.proxies.values())).protocol, 'http')
        r = proxies(json.dumps({'ip':'8.8.8.8','port':443,'protocol':'vless',
                                'connect_string':'vless://sample-id@8.8.8.8:443?security=tls'}))
        self.assertEqual(len(r.proxies), 1)
        self.assertEqual(next(iter(r.proxies.values())).settings['uuid'], 'sample-id')

    def test_uri_country_suffix(self):
        r = proxies('http://8.8.8.8:8080:US\nsocks5://1.1.1.1:1080:DE')
        self.assertEqual(len(r.proxies), 2)

    def test_headerless_csv_and_checked_list_columns(self):
        r = proxies('socks5://8.8.8.8:1080,us,false\nhttp://1.1.1.1:8080,de,true')
        self.assertEqual(len(r.proxies), 2)
        self.assertEqual(r.formats['csv_without_header'], 1)
        r = proxies('http US 0.12s 8.8.8.8:8080\nsocks5 GB 2.05s 1.1.1.1:1080')
        self.assertEqual({p.protocol for p in r.proxies.values()}, {'http','socks5'})

    def test_csv_protocol_columns_and_combined_address_headers(self):
        r = proxies('http,https,socks4,socks5\n8.8.8.8:80,1.1.1.1:443,9.9.9.9:1080,8.8.4.4:1080')
        self.assertEqual({p.protocol for p in r.proxies.values()}, {'http','https','socks4','socks5'})
        r = proxies('protocol,proxy_address,username,password,latency_ms\nsocks5,8.8.8.8:1080,alice,secret,20')
        self.assertEqual(next(iter(r.proxies.values())).settings, {'username':'alice','password':'secret'})
        r = proxies('ip:port,protocol,country\n1.1.1.1:8080,http,United States')
        self.assertEqual(next(iter(r.proxies.values())).protocol, 'http')

    def test_country_and_protocol_annotations_preserve_credentials(self):
        r = proxies('8.8.8.8:8080:United States\n1.1.1.1:3128@HTTP\n9.9.9.9:1080|socks5\n8.8.4.4:80:user:password')
        self.assertEqual(len(r.proxies), 4)
        self.assertIn({'username':'user','password':'password'}, [p.settings for p in r.proxies.values()])

    def test_labeled_and_quoted_list_entries(self):
        r = proxies("🇺🇸 8.8.8.8:80 202ms US [Provider]\nStation #1 -> 1.1.1.1:8100\n'http' : 'http://9.9.9.9:3128',")
        self.assertEqual(len(r.proxies), 3)
        r = proxies('http://user:pa\'ss@8.8.8.8:80')
        self.assertEqual(next(iter(r.proxies.values())).settings['password'], "pa'ss")

    def test_protocol_yaml_and_python_literals(self):
        r = proxies('---\nhttp:\n- "8.8.8.8:80"\nsocks5:\n- "1.1.1.1:1080"')
        self.assertEqual({p.protocol for p in r.proxies.values()}, {'http','socks5'})
        r = proxies('{"http":{"8.8.8.8:80":{"country":None,"active":True}}}')
        self.assertEqual(next(iter(r.proxies.values())).protocol, 'http')
        r = proxies('{"x":None,"value":__import__("os").system("exit 1")}')
        self.assertEqual(len(r.proxies), 0)

    def test_configuration_metadata_is_not_a_proxy(self):
        r = proxies('''proxies:
  - {type: http, server: 8.8.8.8, port: 8080}
dns:
  nameserver: ["1.1.1.1:53"]
proxy-groups:
  - {type: select, proxies: ["9.9.9.9:1234"]}
''')
        self.assertEqual(len(r.proxies), 1)
        self.assertEqual(next(iter(r.proxies.values())).address, '8.8.8.8')
        r = proxies('{"outbounds":[{"type":"dns","server":"1.1.1.1","server_port":53}]}')
        self.assertEqual(len(r.proxies), 0)
        r = proxies('{"ip":"8.8.8.8","port":80,"protocol":["http","socks5"]}')
        self.assertEqual(len(r.proxies), 2)

    def test_json_body_line_breaks_and_hysteria_alias(self):
        r = proxies(json.dumps({'body':'8.8.8.8:80<br>\n1.1.1.1:1080<br />'}))
        self.assertEqual(len(r.proxies), 2)
        r = proxies('hy://8.8.8.8:443?auth=secret')
        p = next(iter(r.proxies.values()))
        self.assertEqual(p.protocol, 'hysteria')
        self.assertEqual(p.settings, {'auth':'secret'})

    def test_multiple_yaml_documents_and_recovery(self):
        good = 'proxies:\n  - {name: a, type: http, server: 8.8.8.8, port: 80}\n'
        r = proxies(good+'---\n'+good)
        self.assertEqual(len(r.proxies), 1)
        self.assertEqual(r.entries_found, 2)
        r = proxies(good+'  - {name: "bad\x00name", type: http, server: 1.1.1.1, port: 80}\n')
        self.assertEqual(len(r.proxies), 1)
        self.assertEqual(r.invalid['invalid_yaml_record'], 1)
        r = proxies(good+'  - name: bad\x9fname\n    type: http\n    server: 1.1.1.1\n    port: 80\n')
        self.assertEqual(len(r.proxies), 2)

    def test_plain_ssr_payload_and_fixed_warp_endpoint(self):
        ssr = '8.8.8.8:443:origin:aes-256-cfb:plain:' + b64('password') + '/?remarks=label'
        r = proxies('ssr://'+ssr+'\nwarp://1.1.1.1:2408?ifp=1-2\nwarp://auto?ifp=1-2')
        self.assertEqual(len(r.proxies), 2)
        self.assertEqual(r.invalid['invalid_address_or_port'], 1)

    def test_yaml_preserves_connection_options_and_ignores_labels(self):
        r = proxies('''proxies:
  - name: first
    type: vless
    server: 8.8.8.8
    port: 443
    uuid: example-id
    tls: true
    reality-opts: {public-key: key, short-id: abc}
  - name: renamed
    type: vless
    server: 8.8.8.8
    port: 443
    uuid: example-id
    tls: true
    reality-opts: {public-key: key, short-id: abc}
''')
        self.assertEqual(r.entries_found, 2)
        self.assertEqual(len(r.proxies), 1)
        p = next(iter(r.proxies.values()))
        self.assertEqual(p.settings['reality-opts']['public-key'], 'key')
        self.assertNotIn('name', p.settings)

    def test_recursive_yaml_is_bounded(self):
        r = proxies('proxies: &recursive [*recursive]')
        self.assertEqual(len(r.proxies), 0)
        self.assertTrue(r.warnings['recursive_or_deep_structure'])

    def test_yaml_python_tags_never_execute(self):
        r = proxies('proxies: !!python/object/apply:os.system ["exit 1"]')
        self.assertEqual(len(r.proxies), 0)
        self.assertTrue(r.warnings['invalid_yaml'])

    def test_vmess_and_base64_subscription(self):
        node = {'v':'2','ps':'label','add':'8.8.8.8','port':'443','id':'sample-id','net':'ws','tls':'tls','path':'/ws'}
        uri = 'vmess://' + b64(json.dumps(node))
        r = proxies(b64(uri+'\nvless://other-id@1.1.1.1:443?security=tls&type=ws#label'))
        self.assertEqual(len(r.proxies), 2)
        vmess = next(p for p in r.proxies.values() if p.protocol == 'vmess')
        self.assertEqual(vmess.settings['uuid'], 'sample-id')
        self.assertEqual(vmess.settings['path'], '/ws')

    def test_shadowsocks_uri_variants(self):
        auth = b64('aes-256-gcm:password')
        whole = b64('aes-256-gcm:password@8.8.8.8:8388')
        r = proxies(f'ss://{auth}@8.8.8.8:8388#one\nss://{whole}#two\n')
        self.assertEqual(r.entries_found, 2)
        self.assertEqual(len(r.proxies), 1)
        self.assertEqual(next(iter(r.proxies.values())).settings['cipher'], 'aes-256-gcm')

    def test_telegram_requires_and_preserves_secret(self):
        r = proxies('tg://proxy?server=8.8.8.8&port=443&secret=abcdef\nhttps://t.me/proxy?server=1.1.1.1&port=443&secret=aaaa\ntg://proxy?server=8.8.8.8&port=443')
        self.assertEqual(len(r.proxies), 2)
        self.assertTrue(all(p.protocol == 'mtproto' and p.settings['secret'] for p in r.proxies.values()))
        self.assertEqual(r.invalid['invalid_uri'], 1)

    def test_xml_and_entity_rejection(self):
        r = proxies('<proxies><proxy><ip>8.8.8.8</ip><port>80</port><protocol>http</protocol></proxy></proxies>')
        self.assertEqual(len(r.proxies), 1)
        r = proxies('<proxies><proxy>8.8.8.8:8080</proxy><proxy>1.1.1.1:1080</proxy></proxies>')
        self.assertEqual(len(r.proxies), 2)
        r = proxies('<!DOCTYPE foo [<!ENTITY x SYSTEM "file:///etc/passwd">]><foo>&x;</foo>')
        self.assertEqual(len(r.proxies), 0)
        self.assertTrue(r.warnings['xml_external_declaration'])

    def test_sing_box_and_v2ray_configuration(self):
        r = proxies(json.dumps({'outbounds':[
            {'type':'vless','server':'8.8.8.8','server_port':443,'uuid':'one','tls':{'enabled':True},'tag':'label'},
            {'protocol':'vmess','settings':{'vnext':[{'address':'1.1.1.1','port':443,'users':[{'id':'two'}]}]},'streamSettings':{'network':'ws'}},
            {'type':'direct','tag':'direct'}]}))
        self.assertEqual(len(r.proxies), 2)
        self.assertTrue(any(p.settings.get('streamSettings') == {'network':'ws'} for p in r.proxies.values()))


if __name__ == '__main__':
    unittest.main()
