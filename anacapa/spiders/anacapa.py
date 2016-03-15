# -*- coding: utf-8 -*-
import os
import itertools
import scrapy

from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from neo4jrestclient.client import GraphDatabase

class AnacapaSpider(scrapy.Spider):
    name  = "anacapa"
    conf  = os.path.join(os.path.dirname(__file__), 'conf')
    rules = [Rule(LinkExtractor(allow=['']), 'parse')]

    def __init__(self):
        self.__init_start_urls()
        self.__init_allowed_domains()
        self.__init_graph()

    def __init_start_urls(self):
        with open(os.path.join(self.conf, 'start_urls.conf'), 'r') as fd:
            self.start_urls = (p for p in fd.read().splitlines() if p)

    def __init_allowed_domains(self):
        with open(os.path.join(self.conf, 'allowed_domains.conf'), 'r') as fd:
            self.allowed_domains = (p for p in fd.read().splitlines() if p)

    def __init_graph(self): 
        self.db    = GraphDatabase("http://localhost:7474", 
                                   username = "neo4j", 
                                   password = "anacapa")

        self.urls  = self.db.labels.create("URL")

    def get_or_create_url(self, url):
        url_nodes = self.urls.get(url = url)

        if len(url_nodes) == 1:
            return url_nodes[0]
        elif url_nodes:
            raise LookupError("Multiple URL nodes found")
        else:
            node = self.db.node.create(url = url)
            self.urls.add(node)
            return node

    def create_redirect_relationship(self, u1, u2):
        if u2 in [rel.end for rel in u1.relationships.outgoing(types = ['REDIRECT'])]:
            return

        u1.relationships.create("REDIRECT", u2)

    def parse_redirect(self, response):
        chain = response.meta['redirect_urls'] + [response.url, ]

        i = 0
        while i < len(chain) - 1:
            u1 = self.get_or_create_url(url = chain[i])
            u2 = self.get_or_create_url(url = chain[i + 1])
            self.create_redirect_relationship(u1, u2)
            i += 1

    def parse_url(self, response):
        u = self.db.node.create(url = response.url)
        self.urls.add(u)

    def parse(self, response):
        if 'redirect_urls' in response.meta:
            self.parse_redirect(response)
            output = "[REDIRECTION] "
            for url in response.meta['redirect_urls']:
                output += "%s -> " % (url, )
                
        else:
            self.parse_url(response)
            output = "[URL] "

        output += response.url
        print(output)
