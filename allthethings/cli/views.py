import os
import json
import orjson
import re
import zlib
import isbnlib
import httpx
import functools
import collections
import barcode
import io
import langcodes
import tqdm
import concurrent
import threading
import yappi
import multiprocessing
import langdetect
import gc
import random
import slugify
import elasticsearch.helpers
import time
import pathlib

from config import settings
from flask import Blueprint, __version__, render_template, make_response, redirect, request
from allthethings.extensions import db, es, Reflected
from sqlalchemy import select, func, text, create_engine
from sqlalchemy.dialects.mysql import match
from pymysql.constants import CLIENT

from allthethings.page.views import elastic_generate_computed_file_info_internal

cli = Blueprint("cli", __name__, template_folder="templates")

# ./run flask cli dbreset
@cli.cli.command('dbreset')
def dbreset():
    print("Erasing entire database! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    es.options(ignore_status=[400,404]).indices.delete(index='computed_search_md5_objs')
    es.indices.create(index='computed_search_md5_objs', body={
        "mappings": {
            "properties": {
              "json":   { "type": "text"  }     
            }
        },
        "settings": {
            "index": { 
                "number_of_replicas": 0,
                "search.slowlog.threshold.query.warn": "2s",
                "store.preload": ["nvd", "dvd"]
            }
        }
    })

    # Per https://stackoverflow.com/a/4060259
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = engine.raw_connection().cursor()

    # Generated with `docker-compose exec mariadb mysqldump -u allthethings -ppassword --opt --where="1 limit 100" --skip-comments --ignore-table=computed_all_md5s allthethings > dump.sql`
    cursor.execute(pathlib.Path(os.path.join(__location__, 'dump.sql')).read_text())

    sql = """
        DROP TABLE IF EXISTS `computed_all_md5s`;
        CREATE TABLE computed_all_md5s (
            md5 CHAR(32) NOT NULL,
            PRIMARY KEY (md5)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 SELECT md5 FROM libgenli_files;
        INSERT IGNORE INTO computed_all_md5s SELECT md5 FROM zlib_book WHERE md5 != '';
        INSERT IGNORE INTO computed_all_md5s SELECT md5_reported FROM zlib_book WHERE md5_reported != '';
        INSERT IGNORE INTO computed_all_md5s SELECT MD5 FROM libgenrs_updated;
        INSERT IGNORE INTO computed_all_md5s SELECT MD5 FROM libgenrs_fiction;
    """
    cursor.execute(sql)
    cursor.close()

    time.sleep(1)
    Reflected.prepare(db.engine)
    elastic_generate_computed_file_info_internal()

    print("Done! Search for example for 'Rhythms of the brain': http://localhost:8000/search?q=Rhythms+of+the+brain")
