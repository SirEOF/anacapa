# -*- coding: utf-8 -*-
import os
import itertools

import logging
log = logging.getLogger("anacapa")

import scrapy

from scrapy.exceptions import CloseSpider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor

from py2neo import neo4j

try:
    import configparser as ConfigParser
except ImportError:
    import ConfigParser


class AnacapaSpider(scrapy.Spider):
    name  = "anacapa"
    conf  = os.path.join(os.path.dirname(__file__), 'conf')
    rules = [Rule(LinkExtractor(allow=['']), callback = 'parse')]

    def __init__(self):
        self.running = True

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
        config = ConfigParser.ConfigParser()

        conf_file = os.path.join(self.conf, 'neo4j.conf')
        if not os.path.exists(conf_file):
            conf_file = os.path.join(self.conf, 'neo4j.conf.default')

        if not os.path.exists(conf_file):
            log.critical("Neo4j not initialized (configuration file not found)")
            self.running = False
            return

        config.read(conf_file)

        try:
            section = config.options('neo4j')
        except:
            log.critical("Neo4j configuration file lacks neo4j section")
            self.running = False
            return

        neo4j.authenticate(config.get('neo4j', 'host'),
                           config.get('neo4j', 'username'),
                           config.get('neo4j', 'password'))

        self.graph = neo4j.Graph(config.get('neo4j', 'url'))

    def parse_redirect(self, response):
        chain = response.meta['redirect_urls'] + [response.url, ]

        i = 0
        while i < len(chain) - 1:
            u1 = self.graph.merge_one("URL", "URL", chain[i])
            u2 = self.graph.merge_one("URL", "URL", chain[i + 1])
            self.graph.create_unique(neo4j.Relationship(u1, "REDIRECT", u2))
            i += 1

    def parse_url(self, response):
        self.graph.merge_one("URL", "URL", response.url)

    def parse(self, response):
        if not self.running:
            raise CloseSpider

        if 'redirect_urls' in response.meta:
            self.parse_redirect(response)
            output = "[REDIRECTION] "
            for url in response.meta['redirect_urls']:
                output += "%s -> " % (url, )        
        else:
            self.parse_url(response)
            output = "[URL] "

        output += response.url
        log.debug(output)
