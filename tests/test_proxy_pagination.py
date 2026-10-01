import unittest
from urllib.parse import parse_qs, urlsplit

from proxy_pagination import next_page


class PaginationTests(unittest.TestCase):
    def test_geonode_keeps_filters_and_walks_to_end(self):
        url = 'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&protocols=http'
        second = next_page(url, {'data':[], 'total':981, 'page':1, 'limit':500})
        self.assertEqual(parse_qs(urlsplit(second).query)['page'], ['2'])
        self.assertEqual(parse_qs(urlsplit(second).query)['protocols'], ['http'])
        self.assertIsNone(next_page(second, {'data':[], 'total':981, 'page':2, 'limit':500}))

    def test_larger_pages_restart_at_first_page_without_skipping_records(self):
        url = 'https://api.socks5proxies.com/api/proxies?limit=25'
        nxt = next_page(url, {'data':[{}]*25,'meta':{'total':90000,'offset':0,'limit':25}})
        self.assertEqual(parse_qs(urlsplit(nxt).query), {'limit':['100'],'offset':['0']})

    def test_freeproxydb_documented_page_parameters(self):
        url = 'https://freeproxydb.com/api/proxy/search?protocol=socks5&page_size=100'
        nxt = next_page(url, {'data':{'total_count':130,'data':[{}]*100}})
        self.assertEqual(parse_qs(urlsplit(nxt).query)['page_index'], ['2'])
        self.assertIsNone(next_page(nxt, {'data':{'total_count':130,'data':[{}]*30}}))

    def test_cursor_is_preserved_and_not_mixed_with_page(self):
        url = 'https://proxylister.com/api/v1/proxies?cursor=previous&limit=100&protocol=socks5'
        nxt = next_page(url, {'results':[{}], 'next':'opaque==/value+', 'limit':100})
        query = parse_qs(urlsplit(nxt).query)
        self.assertEqual(query['cursor'], ['opaque==/value+'])
        self.assertNotIn('page',query)

    def test_external_next_link_is_not_followed(self):
        self.assertIsNone(next_page('https://list.example.com/api', {'next':'http://127.0.0.1/private'}))


if __name__ == '__main__':
    unittest.main()
