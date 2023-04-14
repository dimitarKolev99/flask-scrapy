import os
from flask import Flask, request, jsonify, Response
from sqlalchemy import create_engine, Column, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base
from scrapy.crawler import CrawlerRunner
from types import MethodType
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


Base = declarative_base()


class DynamicModel(Base):
    __tablename__ = "dynamic_table"

    id = Column(Integer, primary_key=True)

    def __init__(self, **kwargs):
        for col in self.__table__.columns:
            if col.name in kwargs:
                setattr(self, col.name, kwargs[col.name])


@app.route('/model', methods=['POST'])
def create_dynamic_model():
    req_data = request.get_json()

    table_name = req_data['table_name']
    column_definitions = json.loads(req_data['column_definitions'])

    columns = {}
    for col_name, col_type in column_definitions.items():
        columns[col_name] = Column(col_name, eval(col_type))

    DynamicModel.__table__ = Table(
        table_name,
        Base.metadata,
        *columns.values(),
    )

    # Modify the following line to use your PostgreSQL database instead of SQLite
    engine = create_engine('sqlite:///dynamic_model.db')
    Base.metadata.create_all(engine)

    return jsonify({
        'status': 'success',
        'message': f'Dynamic model {table_name} created successfully'
    })


def wait_random():
    wait_time = random.uniform(5, 10)
    time.sleep(wait_time)


# def process_results(results):
#     extracted_texts = []
#     for result in results:
#         extracted_text = result.get('text')
#         if extracted_text:
#             extracted_texts.append(extracted_text)
#             print(f"Extracted text: {extracted_text}")
#             # insert the extracted text into the database
#             # cur.execute("INSERT INTO mytable (text_column_name) VALUES (%s)", (extracted_text,))
#             # conn.commit()
#             # cur.close()
#             # conn.close()
#     return extracted_texts


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
            for key, selector in self.parsing_logic.items():
                extracted_data = response.css(selector).get()
                self.log.debug(f"Extracted {key}: {extracted_data}")
                parsed_data[key] = extracted_data

                print("Parsed data: ", parsed_data)
                print("Selector: ", selector)
            yield parsed_data
        except Exception as e:
            self.log.error("Error parsing response: %s", e)
            raise CloseSpider(f"Error parsing response: {e}")

        next_page = None
        if "next_button" in self.next_button and self.page_count <= int(self.max_pages):
            next_page = response.css(
                self.next_button.get("next_button")).get()

        if next_page is not None:
            next_page = response.urljoin(next_page)
            self.page_count += 1

            wait_random()
            yield scrapy.Request(next_page, callback=self.parse)

crawl_runner = CrawlerRunner()      # requires the Twisted reactor to run
quotes_list = []                    # store quotes
scrape_in_progress = False
scrape_complete = False
table_name = None
column_definitions = None

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
        global quotes_list
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


    # Connect to the PostgreSQL database
    # conn = psycopg2.connect(
    #     database="postgres",
    #     user="postgres",
    #     password="postgres",
    #     host="localhost",
    #     port="5433"
    # )

    # Open a cursor to perform database operations
    # cur = conn.cursor()

    print("IN CALLBACK")

    extracted_texts = []
    response = { 'status': 'no data' }

    if table_name is not None and column_definitions is not None:
        columns = {}
        for col_name, col_type in column_definitions.items():
            columns[col_name] = Column(col_name, eval(col_type))

        DynamicModel.__table__ = Table(
            table_name,
            Base.metadata,
            *columns.values(),
        )

        engine = create_engine('postgresql://postgres:postgres@localhost:5433/postgres')
        Base.metadata.create_all(engine)

    if results is not None:
        try:
            for result in results:
                extracted_text = result.get('text')
                if extracted_text:
                    extracted_texts.append(extracted_text)
                    print(f"Extracted text: {extracted_text}")
                    # Insert the extracted text into the database
                    # cur.execute("INSERT INTO mytable (text_column_name) VALUES (%s)", (extracted_text,))
                    # conn.commit()

            # Close the cursor and the database connection
            # cur.close()
            # conn.close()

            # Create a JSON object with the extracted texts and a status message
            response = {'status': 'success', 'extracted_texts': extracted_texts}

        except psycopg2.Error as e:
            # Rollback the transaction and close the database connection
            # conn.rollback()
            # cur.close()
            # conn.close()
            print(f"Error inserting data into the database: {e}")
            # Create a JSON object with an error message
            response = {'status': 'error', 'message': str(e)}

    # Convert the response object to a JSON string and return it
    return json.dumps(response)


if __name__ == '__main__':
    configure_logging()
    app.run('0.0.0.0', 9000)

# @app.route('/results')
# def get_results():
#     """
#     Get the results only if a spider has results
#     """
#     global scrape_complete
#     if scrape_complete:
#         return json.dumps(quotes_list)
#     return 'Scrape Still Progress'

# @app.route('/scrape', methods=['POST'])
# def scrape():
#     req_data = request.get_json()
#     url = req_data['url']
#     parsing_logic = req_data['parsing_logic']
#     max_pages = req_data['max_pages']

#     configure_logging()
#     runner = CrawlerRunner(get_project_settings())

#     @defer.inlineCallbacks
#     def crawl():
#         yield runner.crawl(MySpider, url=url, parsing_logic=parsing_logic, max_pages=max_pages)
#         reactor.stop()


#     d = crawl()
#     d.addCallback(process_results)
#     reactor.run()

#     return 'Scraper started!'

# if __name__ == '__main__':
#     configure_logging()
#     app.run()
