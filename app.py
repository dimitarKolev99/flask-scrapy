import os
from flask import Flask, request, jsonify, Response
from sqlalchemy import create_engine, Column, Integer, String, Table, MetaData, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert
from scrapy.crawler import CrawlerRunner
from types import MethodType
import requests
import config
import json
import scrapy
import psycopg2
import logging
from scrapy.utils.log import configure_logging
from twisted.internet import reactor, defer, task
from twisted.python.failure import Failure
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import CloseSpider
import time
import random
import crochet
import json
from flask import Flask
from scrapy.crawler import CrawlerRunner
crochet.setup()     # initialize crochet

app = Flask(__name__)

crawl_runner = CrawlerRunner()      # requires the Twisted reactor to run
quotes_list = []                    # store quotes
title_list = []
scrape_in_progress = False
scrape_complete = False
table_name = None
column_definitions = None
html_string = None

@app.route('/log')
def log():
    with open(os.path.join(os.getcwd(), 'spider.log'), 'r') as f:
        log_contents = f.read()
    return log_contents


# Configure logging
configure_logging(install_root_handler=False)
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.INFO
)


def wait_random():
    wait_time = random.uniform(5, 10)
    time.sleep(wait_time)


class MySpider(scrapy.Spider):
    name = 'myspider'

    def __init__(self, url, parsing_logic, max_pages, next_button, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        self.parsing_logic = parsing_logic
        self.start_urls = [url]
        self.page_count = 1
        self.max_pages = max_pages
        self.next_button = next_button
        self.log = logging.getLogger(self.name)

        handler = logging.FileHandler('spider.log')
        handler.setLevel(logging.DEBUG)
        self.log.addHandler(handler)
        self.log.debug(
            "MySpider initialized with parsing logic: %s", parsing_logic)

    def parse(self, response):
        self.log.debug("parse() method called with response: %s", response.url)
        try:
            parsed_data = {}
            global title_list
            if self.parsing_logic is not None:
                for key, selector in self.parsing_logic.items():
                    extracted_data = response.css(selector).get()

                    self.log.debug(f"Extracted {key}: {extracted_data}")

                    parsed_data[key] = extracted_data
                    title_list.append(parsed_data)

                yield parsed_data
            else:
                global html_string
                html_string = response.text
                return
        except Exception as e:
            self.log.error("Error parsing response: %s", e)
            raise CloseSpider(f"Error parsing response: {e}")

        next_page = None
        if self.next_button is not None and "next_button" in self.next_button and self.page_count <= int(self.max_pages):
            next_page = response.css(
                self.next_button.get("next_button")).get()

        if next_page is not None:
            next_page = response.urljoin(next_page)
            self.page_count += 1

            wait_random()
            yield scrapy.Request(next_page, callback=self.parse)

@app.route('/html', methods=['POST'])
def get_html():
    global html_string
    global scrape_in_progress
    global scrape_complete

    req_data = request.get_json()
    url = req_data['url']

    if not scrape_in_progress:
        scrape_in_progress = True
        # start the crawler and execute a callback when complete
        scrape_with_crochet(
            url=url, parsing_logic=None, max_pages=None, next_button=None)
        return 'SCRAPING'

    return 'SCRAPE IN PROGRESS'



@app.route('/crawl', methods=['POST'])
def crawl_for_quotes():
    """
    Scrape for quotes
    """
    global scrape_in_progress
    global scrape_complete
    global table_name
    global column_definitions

    req_data = request.get_json()
    url = req_data['url']
    parsing_logic = req_data['parsing_logic']
    max_pages = req_data['max_pages']
    next_button = req_data['next_button']
    table_name = req_data['table_name']
    column_definitions = req_data['column_definitions']

    table_name = table_name
    column_definitions = column_definitions

    configure_logging()

    if not scrape_in_progress:
        scrape_in_progress = True
        # start the crawler and execute a callback when complete
        scrape_with_crochet(
            url=url, parsing_logic=parsing_logic, max_pages=max_pages, next_button=next_button)
        return 'SCRAPING'

    return 'SCRAPE IN PROGRESS'


@crochet.run_in_reactor
def scrape_with_crochet(url, parsing_logic, max_pages, next_button):
    eventual = crawl_runner.crawl(
        MySpider, url=url, parsing_logic=parsing_logic, max_pages=max_pages, next_button=next_button)
    eventual.addCallback(finished_scrape)


def finished_scrape(results):
    """
    A callback that is fired after the scrape has completed.
    Set a flag to allow display the results from /results
    """
    global scrape_complete
    scrape_complete = True
    global scrape_in_progress
    scrape_in_progress = False
    global table_name
    global column_definitions
    global title_list
    global html_string

    a = {
        "data": title_list
    }

    b = {
        "data": html_string
    }

    if title_list:
        send_webhook(a)
    elif html_string:
        send_webhook(b)
   
def send_webhook(msg):
    """
    Send a webhook to a specified URL
    :param msg: task details
    :return:
    """
    print("MSG: ", json.dumps(msg))

    headers = {
                "Connection": "close",
               "X-Requested-With": "XMLHttpRequest",
               "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.52 Safari/536.5",
               "Content-Type": "application/json",
               "Accept": "*/*",
               }

    try:
        # Post a webhook message
        # default is a function applied to objects that are not serializable = it converts them to str
        resp = requests.post("http://localhost:8080/scraped",
                             headers=headers,
                             data=json.dumps(msg))
        # Returns an HTTPError if an error has occurred during the process (used for debugging).
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        # print("An HTTP Error occurred",repr(err))
        pass
    except requests.exceptions.ConnectionError as err:
        # print("An Error Connecting to the API occurred", repr(err))
        pass
    except requests.exceptions.Timeout as err:
        # print("A Timeout Error occurred", repr(err))
        pass
    except requests.exceptions.RequestException as err:
        # print("An Unknown Error occurred", repr(err))
        pass
    except:
        pass
    else:
        print("RESPONSE: ", {"status": resp.status_code, "body": resp.content})
        return {"status": resp.status_code, "body": resp.json}


if __name__ == '__main__':
    configure_logging()
    app.run('0.0.0.0', 9000)

