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

from config import settings
from flask import Blueprint, __version__, render_template, make_response, redirect, request
from allthethings.extensions import db, es
from sqlalchemy import select, func, text, create_engine
from sqlalchemy.dialects.mysql import match
from pymysql.constants import CLIENT

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

    sql = """
        SET FOREIGN_KEY_CHECKS = 0;
        DROP TABLE IF EXISTS `computed_all_md5s`;
        DROP TABLE IF EXISTS `computed_search_md5_objs`;
        DROP TABLE IF EXISTS `isbndb_isbns`;
        DROP TABLE IF EXISTS `libgenli_editions`;
        DROP TABLE IF EXISTS `libgenli_editions_add_descr`;
        DROP TABLE IF EXISTS `libgenli_editions_to_files`;
        DROP TABLE IF EXISTS `libgenli_elem_descr`;
        DROP TABLE IF EXISTS `libgenli_files`;
        DROP TABLE IF EXISTS `libgenli_files_add_descr`;
        DROP TABLE IF EXISTS `libgenli_publishers`;
        DROP TABLE IF EXISTS `libgenli_series`;
        DROP TABLE IF EXISTS `libgenli_series_add_descr`;
        DROP TABLE IF EXISTS `libgenrs_description`;
        DROP TABLE IF EXISTS `libgenrs_fiction`;
        DROP TABLE IF EXISTS `libgenrs_fiction_description`;
        DROP TABLE IF EXISTS `libgenrs_fiction_hashes`;
        DROP TABLE IF EXISTS `libgenrs_hashes`;
        DROP TABLE IF EXISTS `libgenrs_topics`;
        DROP TABLE IF EXISTS `libgenrs_updated`;
        DROP TABLE IF EXISTS `ol_base`;
        DROP TABLE IF EXISTS `ol_isbn13`;
        DROP TABLE IF EXISTS `zlib_book`;
        DROP TABLE IF EXISTS `zlib_ipfs`;
        DROP TABLE IF EXISTS `zlib_isbn`;
        SET FOREIGN_KEY_CHECKS = 1;

        CREATE TABLE `computed_all_md5s` (
          `md5` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
          PRIMARY KEY (`md5`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        CREATE TABLE `computed_search_md5_objs` (
          `md5` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
          `json` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
          PRIMARY KEY (`md5`),
          FULLTEXT KEY `json` (`json`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        CREATE TABLE `isbndb_isbns` (
          `isbn13` char(13) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
          `isbn10` char(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
          `json` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`json`)),
          PRIMARY KEY (`isbn13`,`isbn10`),
          KEY `isbn10` (`isbn10`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        CREATE TABLE `libgenli_editions` (
          `e_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `libgen_topic` enum('a','s','l','f','r','m','c') COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'раздел LG',
          `type` enum('','b','ch','bpart','bsect','bs','bset','btrack','component','dataset','diss','j','a','ji','jv','mon','oth','peer-review','posted-content','proc','proca','ref','refent','rep','repser','s','fnz','m','col','chb','nonfict','omni','nov','ant','c') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `series_name` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `title` varchar(2000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Заголовок',
          `title_add` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Дополнение к заглавию',
          `author` varchar(2000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `publisher` varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `city` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Город',
          `edition` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `year` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Год',
          `month` enum('','1q','1s','1t','2q','2s','2t','3q','3t','4q','4t','apr','aug','chr','dec','fal','feb','hol','jan','jul','jun','mar','may','mon','nov','oct','sep','spr','sum','win','aut') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `day` varchar(2) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'День издания',
          `pages` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `editions_add_info` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Библиографический комментарий к изданию (вид выпуска -  спец., ежегодник (если не выделены  в отдельную подшивку))',
          `cover_url` varchar(450) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Ссылка на обложку',
          `cover_exists` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Наличие обложки в репозитории lg editions',
          `issue_s_id` int(11) NOT NULL DEFAULT 0 COMMENT 'Ссылка на таблицу series для периодических изданий',
          `issue_number_in_year` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Техническая нумерация в году для сортировки',
          `issue_year_number` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Номер за год',
          `issue_number` varchar(95) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Номер выпуска (в рамках тома)',
          `issue_volume` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Том',
          `issue_split` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Признак того, что номер сдвоен, 0-не сдвоен, 1,2,3 - с каким числом номеров сдвоен',
          `issue_total_number` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Сквозная нумерация всей подшивки',
          `issue_first_page` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `issue_last_page` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `issue_year_end` varchar(4) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Конечный год, заполняется если номер сдвоенный или приходится на границу 2-х годов',
          `issue_month_end` enum('','1q','1s','1t','2q','2s','2t','3q','3t','4q','4t','apr','aug','chr','dec','fal','feb','hol','jan','jul','jun','mar','may','mon','nov','oct','sep','spr','sum','win','aut') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `issue_day_end` varchar(2) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Конечный день, заполняется если номер сдвоенный или приходится на границу 2-х годов',
          `issue_closed` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Если номер нe издавался 0, иначе=1',
          `doi` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `full_text` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
          `time_added` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT 'Дата добавления',
          `time_last_modified` timestamp NOT NULL DEFAULT current_timestamp() COMMENT 'Дата последнего изменения',
          `visible` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Видимое, или закрыто для просмотра по разным причинам',
          `editable` tinyint(1) NOT NULL DEFAULT 1 COMMENT 'Возможность редактирования пользователями',
          `uid` int(10) unsigned NOT NULL DEFAULT 0,
          `commentary` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          PRIMARY KEY (`e_id`) USING BTREE
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Издания, в.т.ч. периодические';

        CREATE TABLE `libgenli_editions_add_descr` (
          `e_add_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `e_id` int(10) unsigned NOT NULL DEFAULT 0,
          `key` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Ссылка на описание elem_descr ',
          `value` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add1` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add2` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add3` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_hash` bigint(20) unsigned NOT NULL,
          `date_start` date DEFAULT NULL,
          `date_end` date DEFAULT NULL,
          `issue_start` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Начальное издание, при наличие issue_able в elem_descr',
          `issue_end` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Конечное издание, при наличие issue_able в elem_descr',
          `time_added` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
          `time_last_modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
          `commentary` varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `uid` int(11) DEFAULT 0,
          `value_id` bigint(20) NOT NULL DEFAULT 0,
          PRIMARY KEY (`e_add_id`) USING BTREE,
          KEY `EID` (`e_id`) USING BTREE
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Дополнительные элементы описания к изданиям';

        CREATE TABLE `libgenli_editions_to_files` (
          `etf_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `f_id` int(10) unsigned NOT NULL,
          `e_id` int(10) unsigned NOT NULL,
          `time_added` datetime NOT NULL,
          `time_last_modified` datetime NOT NULL,
          `uid` int(10) unsigned NOT NULL DEFAULT 0,
          PRIMARY KEY (`etf_id`) USING BTREE,
          UNIQUE KEY `IDS` (`f_id`,`e_id`),
          KEY `EID` (`e_id`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        CREATE TABLE `libgenli_elem_descr` (
          `key` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `commentary` varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Комментарий',
          `name_ru` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наименование описательного элемента на русском - зависит от языка интерфейса',
          `name_en` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наименование описательного элемента на английском',
          `type` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'тип данных - гиперссылка, xml, ссылка на картинку и пр.',
          `checks` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'проверка значения через регулярные выражения или ссылки на справочники',
          `link_pattern` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Гиперссылка для дополнения id- ссылки на справочник',
          `name_add1_ru` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наименование описательного элемента на русском - зависит от языка интерфейса',
          `name_add1_en` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наименование описательного элемента на английском',
          `type_add1` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'тип данных - гиперссылка, xml, ссылка на картинку и пр.',
          `checks_add1` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'проверка значения через регулярные выражения или ссылки на справочники',
          `filled_add1` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Обязательность заполнения',
          `link_pattern_add1` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Гиперссылка для дополнения id- ссылки на справочник',
          `name_add2_ru` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наименование описательного элемента на русском - зависит от языка интерфейса',
          `name_add2_en` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наименование описательного элемента на английском',
          `type_add2` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'тип данных - гиперссылка, xml, ссылка на картинку и пр.',
          `checks_add2` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'проверка значения через регулярные выражения или ссылки на справочники',
          `filled_add2` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Обязательность заполнения',
          `link_pattern_add2` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Гиперссылка для дополнения id- ссылки на справочник',
          `name_add3_ru` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наименование описательного элемента на русском - зависит от языка интерфейса',
          `name_add3_en` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наименование описательного элемента на английском',
          `type_add3` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'тип данных - гиперссылка, xml, ссылка на картинку и пр.',
          `checks_add3` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'проверка значения через регулярные выражения или ссылки на справочники',
          `filled_add3` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Обязательность заполнения',
          `link_pattern_add3` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Гиперссылка для дополнения id- ссылки на справочник',
          `for_works` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для работ',
          `for_publishers` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для издательств',
          `for_editions` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для изданий',
          `for_authors` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для авторов',
          `for_series` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'для серий',
          `for_files` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для файлов',
          `dateable` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Может ли иметь период действия с - по',
          `issueable` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Может ли иметь период действия с выпуска - по выпуск',
          `default_view_for_edit` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Показывать по умолчанию при редактировании',
          `multiple_values` tinyint(1) NOT NULL DEFAULT 1 COMMENT 'у объекта может быть несколько описательных полей с одним и тем же типом',
          `for_libgen` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для раздела',
          `for_fiction` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для раздела',
          `for_fiction_rus` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для раздела',
          `for_scimag` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для раздела',
          `for_magz` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для раздела',
          `for_standarts` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для раздела',
          `for_comics` tinyint(1) NOT NULL DEFAULT 0 COMMENT 'Для раздела',
          `sort` int(11) NOT NULL DEFAULT 0 COMMENT 'Сортировка',
          `visible` tinyint(1) NOT NULL DEFAULT 1 COMMENT 'Видимое в описани',
          `editable` tinyint(1) NOT NULL DEFAULT 1 COMMENT 'Возможно ручное редактирование пользователем',
          PRIMARY KEY (`key`) USING BTREE,
          UNIQUE KEY `UNIQ1` (`name_ru`),
          UNIQUE KEY `UNIQ2` (`name_en`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Виды элементов описания';

        CREATE TABLE `libgenli_files` (
          `f_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `md5` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
          `pages` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Техническое количество страниц в скане',
          `dpi` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Разрешение',
          `visible` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Видимый, если не пусто, то указывает по каким причинам - cpr -  абуза, del- удален физически, no - прочие причины',
          `time_added` datetime NOT NULL,
          `time_last_modified` datetime NOT NULL,
          `cover_url` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Ссылка на обложку',
          `cover_exists` tinyint(1) NOT NULL DEFAULT 0,
          `commentary` varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Доп. инфо о скане (fixed и пр.)',
          `color` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Цветной',
          `cleaned` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Очищенный скан',
          `orientation` enum('P','L','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Ориентация скана - Портретная, Ландшафтная',
          `paginated` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Разворот разрезан на страницы',
          `scanned` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Сканированный',
          `vector` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Векторный',
          `bookmarked` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Есть оглавление',
          `ocr` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Есть текстовый слой',
          `filesize` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Размер файла',
          `extension` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Расширение',
          `locator` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Имя файла (до загрузки  в репозиторий)',
          `broken` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Битый',
          `editable` tinyint(3) unsigned NOT NULL DEFAULT 1 COMMENT 'Запись редактируема',
          `generic` char(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Ссылка на лучшую версию файла',
          `cover_info` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Информация об обложках (если их несколько)',
          `file_create_date` datetime NOT NULL DEFAULT '2000-01-01 05:00:00' COMMENT 'Техническая дата создания файла',
          `archive_files_count` int(10) unsigned NOT NULL DEFAULT 0,
          `archive_dop_files_flag` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'наличие доп. файлов кроме картинок, для cbr, cbz, rar, zip, 7z',
          `archive_files_pic_count` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Количество картинок в архиве',
          `scan_type` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Тип скана - цифровой, веб, бумажный скан, микропленка',
          `scan_content` varchar(145) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `c2c` enum('Y','N','') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Наличие рекламы в скане (c2c)',
          `scan_quality` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Качество скана (HQ, Q10)',
          `releaser` varchar(125) COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'Автор релиза',
          `libgen_id` int(10) unsigned NOT NULL DEFAULT 0,
          `fiction_id` int(10) unsigned NOT NULL DEFAULT 0,
          `fiction_rus_id` int(10) unsigned NOT NULL DEFAULT 0,
          `comics_id` int(10) unsigned NOT NULL DEFAULT 0,
          `scimag_id` int(10) unsigned NOT NULL DEFAULT 0,
          `standarts_id` int(10) unsigned NOT NULL DEFAULT 0,
          `magz_id` int(10) unsigned NOT NULL DEFAULT 0,
          `libgen_topic` enum('l','s','m','c','f','r','a') COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'правильный раздел для файла',
          `scan_size` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'размер рандомной картинки из архива',
          `scimag_archive_path` varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `scimag_archive_path_is_doi` tinyint(1) DEFAULT 0 COMMENT 'Путь в архиве соответствует doi в editions',
          `uid` int(10) unsigned NOT NULL DEFAULT 0,
          PRIMARY KEY (`f_id`),
          UNIQUE KEY `MD5_UNIQ` (`md5`) USING BTREE
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Файлы';

        CREATE TABLE `libgenli_files_add_descr` (
          `f_add_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `f_id` int(10) unsigned NOT NULL DEFAULT 0,
          `key` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Ссылка на описание elem_descr ',
          `value` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add1` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add2` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add3` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_hash` bigint(20) unsigned NOT NULL,
          `date_start` date DEFAULT NULL,
          `date_end` date DEFAULT NULL,
          `issue_start` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Начальное издание, при наличие issue_able в elem_descr',
          `issue_end` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Конечное издание, при наличие issue_able в elem_descr',
          `time_added` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
          `time_last_modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
          `commentary` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `uid` int(10) unsigned NOT NULL DEFAULT 0,
          PRIMARY KEY (`f_add_id`) USING BTREE,
          UNIQUE KEY `VAL_UNIQ` (`value_hash`,`f_id`,`key`) USING BTREE,
          KEY `F_ID` (`f_id`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Дополнительные элементы описания к сериям';

        CREATE TABLE `libgenli_publishers` (
          `p_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `title` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Название',
          `org_type` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT 'Вид организации',
          `add_info` varchar(45) COLLATE utf8mb4_unicode_ci DEFAULT '',
          `time_added` datetime NOT NULL,
          `time_last_modified` datetime NOT NULL,
          `date_start` date DEFAULT NULL,
          `date_end` date DEFAULT NULL,
          `uid` int(10) unsigned NOT NULL DEFAULT 0,
          `visible` varchar(3) COLLATE utf8mb4_unicode_ci DEFAULT '',
          `editable` tinyint(1) DEFAULT 1,
          `commentary` varchar(45) COLLATE utf8mb4_unicode_ci DEFAULT '',
          PRIMARY KEY (`p_id`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Издательства';

        CREATE TABLE `libgenli_series` (
          `s_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `libgen_topic` enum('s','a','l','f','r','m','c') COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Раздел LG',
          `title` varchar(500) COLLATE utf8mb4_unicode_ci NOT NULL COMMENT 'Заголовок серии',
          `add_info` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT '',
          `type` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Тип серии - mag - журнал com - комикс и т.п.',
          `volume` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Том',
          `volume_type` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Тип серии - HS, INT, Annual, OS и т. п.',
          `volume_name` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Название тома',
          `publisher` varchar(1000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Издательство',
          `commentary` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Комментарий',
          `date_start` date DEFAULT '0000-00-00' COMMENT 'Дата начала издания',
          `date_end` date DEFAULT '9999-00-00' COMMENT 'Дата окончания издания',
          `time_last_modified` datetime NOT NULL COMMENT 'Дата изменения',
          `time_added` datetime NOT NULL COMMENT 'Дата добавления',
          `visible` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Видимая, если пусто - видимая, cpr - копирайт, del - удаленная, dbl -дубль',
          `editable` int(11) DEFAULT 1 COMMENT 'Запрет на редактирование пользователям',
          `uid` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'ID пользователя',
          PRIMARY KEY (`s_id`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Серии';

        CREATE TABLE `libgenli_series_add_descr` (
          `s_add_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `s_id` int(10) unsigned NOT NULL DEFAULT 0,
          `key` int(10) unsigned NOT NULL DEFAULT 0 COMMENT 'Ссылка на описание elem_descr ',
          `value` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add1` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add2` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_add3` mediumtext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `value_hash` bigint(20) unsigned NOT NULL,
          `date_start` date DEFAULT NULL,
          `date_end` date DEFAULT NULL,
          `issue_start` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Начальное издание, при наличие issue_able в elem_descr',
          `issue_end` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '' COMMENT 'Конечное издание, при наличие issue_able в elem_descr',
          `time_added` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
          `time_last_modified` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
          `commentary` varchar(250) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `uid` int(10) unsigned NOT NULL DEFAULT 0,
          PRIMARY KEY (`s_add_id`) USING BTREE,
          UNIQUE KEY `VAL_UNIQ` (`value_hash`,`s_id`,`key`),
          KEY `KEY` (`key`),
          KEY `S_ID` (`s_id`) USING BTREE
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Дополнительные элементы описания к сериям';

        CREATE TABLE `libgenrs_description` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `md5` varchar(32) CHARACTER SET utf8mb3 NOT NULL,
          `descr` mediumtext COLLATE utf8mb4_bin NOT NULL,
          `toc` mediumtext COLLATE utf8mb4_bin NOT NULL,
          `TimeLastModified` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
          PRIMARY KEY (`id`),
          UNIQUE KEY `md5_unique` (`md5`) USING BTREE
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

        CREATE TABLE `libgenrs_fiction` (
          `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `MD5` char(32) CHARACTER SET ascii DEFAULT NULL,
          `Title` varchar(2000) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Author` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Series` varchar(300) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Edition` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Language` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Year` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Publisher` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Pages` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Identifier` varchar(400) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `GooglebookID` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `ASIN` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Coverurl` varchar(200) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Extension` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL,
          `Filesize` int(10) unsigned NOT NULL,
          `Library` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Issue` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Locator` varchar(512) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Commentary` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `Generic` char(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `Visible` char(3) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
          `TimeAdded` timestamp NOT NULL DEFAULT current_timestamp(),
          `TimeLastModified` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
          PRIMARY KEY (`ID`),
          UNIQUE KEY `MD5UNIQUE` (`MD5`) USING BTREE
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        CREATE TABLE `libgenrs_fiction_description` (
          `MD5` char(32) CHARACTER SET ascii NOT NULL,
          `Descr` mediumtext COLLATE utf8mb4_bin NOT NULL,
          `TimeLastModified` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
          PRIMARY KEY (`MD5`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

        CREATE TABLE `libgenrs_fiction_hashes` (
          `md5` char(32) NOT NULL,
          `crc32` char(8) NOT NULL DEFAULT '',
          `edonkey` char(32) NOT NULL DEFAULT '',
          `aich` char(32) NOT NULL DEFAULT '',
          `sha1` char(40) NOT NULL DEFAULT '',
          `tth` char(39) NOT NULL DEFAULT '',
          `btih` char(40) NOT NULL DEFAULT '',
          `sha256` char(64) NOT NULL DEFAULT '',
          `ipfs_cid` char(62) NOT NULL DEFAULT '',
          PRIMARY KEY (`md5`)
        ) ENGINE=MyISAM DEFAULT CHARSET=ascii;

        CREATE TABLE `libgenrs_hashes` (
          `md5` char(32) NOT NULL,
          `crc32` char(8) NOT NULL DEFAULT '',
          `edonkey` char(32) NOT NULL DEFAULT '',
          `aich` char(32) NOT NULL DEFAULT '',
          `sha1` char(40) NOT NULL DEFAULT '',
          `tth` char(39) NOT NULL DEFAULT '',
          `torrent` text DEFAULT NULL,
          `btih` char(40) NOT NULL DEFAULT '',
          `sha256` char(64) NOT NULL DEFAULT '',
          `ipfs_cid` char(62) NOT NULL DEFAULT '',
          PRIMARY KEY (`md5`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb3;

        CREATE TABLE `libgenrs_topics` (
          `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `topic_descr` varchar(500) NOT NULL DEFAULT '',
          `lang` varchar(2) NOT NULL DEFAULT '',
          `kolxoz_code` varchar(10) NOT NULL DEFAULT '',
          `topic_id` int(10) unsigned DEFAULT NULL,
          `topic_id_hl` int(10) unsigned DEFAULT NULL,
          PRIMARY KEY (`id`),
          KEY `LANG` (`lang`) USING BTREE,
          KEY `topic_id+topic_id_hl` (`topic_id`,`topic_id_hl`),
          KEY `topic_id` (`topic_id`),
          KEY `topic_id_hl` (`topic_id_hl`),
          KEY `topic_id+lang` (`topic_id`,`lang`),
          KEY `topic_id_hl+lang` (`topic_id_hl`,`lang`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb3;

        CREATE TABLE `libgenrs_updated` (
          `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
          `Title` varchar(2000) DEFAULT '',
          `VolumeInfo` varchar(100) DEFAULT '',
          `Series` varchar(300) DEFAULT '',
          `Periodical` varchar(200) DEFAULT '',
          `Author` varchar(1000) DEFAULT '',
          `Year` varchar(14) DEFAULT '',
          `Edition` varchar(60) DEFAULT '',
          `Publisher` varchar(400) DEFAULT '',
          `City` varchar(100) DEFAULT '',
          `Pages` varchar(100) DEFAULT '',
          `PagesInFile` int(10) unsigned NOT NULL DEFAULT 0,
          `Language` varchar(150) DEFAULT '',
          `Topic` varchar(500) DEFAULT '',
          `Library` varchar(50) DEFAULT '',
          `Issue` varchar(100) DEFAULT '',
          `Identifier` varchar(300) DEFAULT '',
          `ISSN` varchar(9) DEFAULT '',
          `ASIN` varchar(200) DEFAULT '',
          `UDC` varchar(200) DEFAULT '',
          `LBC` varchar(200) DEFAULT '',
          `DDC` varchar(45) DEFAULT '',
          `LCC` varchar(45) DEFAULT '',
          `Doi` varchar(45) DEFAULT '',
          `Googlebookid` varchar(45) DEFAULT '',
          `OpenLibraryID` varchar(200) DEFAULT '',
          `Commentary` varchar(10000) DEFAULT '',
          `DPI` int(10) unsigned DEFAULT 0,
          `Color` varchar(1) DEFAULT '',
          `Cleaned` varchar(1) DEFAULT '',
          `Orientation` varchar(1) DEFAULT '',
          `Paginated` varchar(1) DEFAULT '',
          `Scanned` varchar(1) DEFAULT '',
          `Bookmarked` varchar(1) DEFAULT '',
          `Searchable` varchar(1) DEFAULT '',
          `Filesize` bigint(20) unsigned NOT NULL DEFAULT 0,
          `Extension` varchar(50) DEFAULT '',
          `MD5` char(32) DEFAULT '',
          `Generic` char(32) DEFAULT '',
          `Visible` char(3) DEFAULT '',
          `Locator` varchar(733) DEFAULT '',
          `Local` int(10) unsigned DEFAULT 0,
          `TimeAdded` timestamp NOT NULL DEFAULT '2000-01-01 05:00:00',
          `TimeLastModified` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
          `Coverurl` varchar(200) DEFAULT '',
          `Tags` varchar(500) DEFAULT '',
          `IdentifierWODash` varchar(300) DEFAULT '',
          PRIMARY KEY (`ID`),
          UNIQUE KEY `MD5` (`MD5`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb3;

        CREATE TABLE `ol_base` (
          `type` char(40) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
          `ol_key` char(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
          `revision` int(11) NOT NULL,
          `last_modified` datetime NOT NULL,
          `json` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`json`)),
          PRIMARY KEY (`ol_key`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        CREATE TABLE `ol_isbn13` (
          `isbn` char(13) COLLATE utf8mb4_unicode_ci NOT NULL,
          `ol_key` char(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
          PRIMARY KEY (`isbn`,`ol_key`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        CREATE TABLE `zlib_book` (
          `zlibrary_id` int(11) NOT NULL,
          `date_added` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `date_modified` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `extension` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `filesize` bigint(20) DEFAULT NULL,
          `filesize_reported` bigint(20) NOT NULL,
          `md5` char(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `md5_reported` char(32) COLLATE utf8mb4_unicode_ci NOT NULL,
          `title` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `author` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `publisher` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `language` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `series` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `volume` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `edition` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `year` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `pages` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `description` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `cover_url` text COLLATE utf8mb4_unicode_ci NOT NULL,
          `in_libgen` tinyint(1) NOT NULL DEFAULT 0,
          `pilimi_torrent` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `unavailable` tinyint(1) NOT NULL DEFAULT 0,
          PRIMARY KEY (`zlibrary_id`),
          KEY `md5` (`md5`),
          KEY `md5_reported` (`md5_reported`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

        CREATE TABLE `zlib_ipfs` (
          `zlibrary_id` int(11) NOT NULL,
          `ipfs_cid` char(62) NOT NULL,
          PRIMARY KEY (`zlibrary_id`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;

        CREATE TABLE `zlib_isbn` (
          `zlibrary_id` int(11) NOT NULL,
          `isbn` varchar(13) COLLATE utf8mb4_unicode_ci NOT NULL,
          PRIMARY KEY (`zlibrary_id`,`isbn`),
          UNIQUE KEY `isbn_id` (`isbn`,`zlibrary_id`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    engine = create_engine(settings.SQLALCHEMY_DATABASE_URI, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = engine.raw_connection().cursor()
    cursor.execute(sql)
