# -*- coding: utf-8 -*-
import os
import csv
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
    tags  = {
		('//script', '@src'   , 'SRC'   , 'SCRIPT'),
                ('//a'     , '@href'  , 'HREF'  , 'A'     ),
                ('//form'  , '@action', 'ACTION', 'FORM'  ),
            }

    def __init__(self):
        self.running = True

        self.__init_start_urls()
        self.__init_allowed_domains()
        self.__init_alexa_domains()
        self.__init_graph()

    def __init_start_urls(self):
        with open(os.path.join(self.conf, 'start_urls.conf'), 'r') as fd:
            self.start_urls = [p for p in fd.read().splitlines() if p]

    def __init_allowed_domains(self):
        with open(os.path.join(self.conf, 'allowed_domains.conf'), 'r') as fd:
            self.allowed_domains = [p for p in fd.read().splitlines() if p]

    def __init_alexa_domains(self):
        self.alexa_domains = list()

        with open(os.path.join(self.conf, 'top-1m.csv'), 'r') as fd:
            reader = csv.reader(fd)
            self.alexa_domains = [p[1] for p in reader if len(p) > 1]

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

    def url_label(self, url):
        url = url.lower()

        for domain in list(self.allowed_domains):
            if domain.lower() in url:
                return "URL_ALLOWED"

        for domain in self.alexa_domains:
            if domain.lower() in url:
                return "URL_ALEXA"

        return "URL_UNKNOWN"

    def handle_url(self, url, labels = None):
        if labels is None:
            labels = list()

        node = self.graph.merge_one("URL", "URL", url)

        node.labels.add(self.url_label(url))
        for l in labels:
            node.labels.add(l)

        self.graph.push(node)
        return node

    def parse_tag(self, response, tag, target, node_type, rel_type):
        url = self.handle_url(response.url)

        for sel in response.xpath(tag):
            for src in sel.xpath(target).extract():
                node = self.handle_url(response.urljoin(src), labels = [node_type, ])
                self.graph.create_unique(neo4j.Relationship(url, rel_type, node))

    def parse_response(self, response):
        for parms in self.tags:
            self.parse_tag(response, *parms)

    def parse_redirect(self, response):
        chain = response.meta['redirect_urls'] + [response.url, ]

        i = 0
        while i < len(chain) - 1:
            u1 = self.handle_url(chain[i])
            u2 = self.handle_url(chain[i + 1])
            self.graph.create_unique(neo4j.Relationship(u1, "REDIRECT", u2))
            i += 1

    def parse_url(self, response):
        self.handle_url(response.url)

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

        self.parse_response(response)
