This should all be properly automated, but here is a rough sketch of the steps to import various data sources so far.

This has not recently been tested, so if you go through this, it would be helpful to take notes, improve this file, or even write some actual automated scripts.

## Z-Library

Get `pilimi-zlib2-index-2022-08-24-fixed.sql` from pilimi.org.

```bash
pv pilimi-zlib2-index-2022-08-24-fixed.sql | 
    sed -e 's/^) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;$/) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;/g' | \
    ./run mysql allthethings
```

```sql
RENAME TABLE books TO zlib_book;
RENAME TABLE isbn TO zlib_isbn;
```

Get `ipfs.csv` from pilimi.org (pilimi-zlib2-derived.torrent).

```sql
CREATE TABLE zlib_ipfs (
  zlibrary_id INT NOT NULL,
  ipfs_cid CHAR(62) NOT NULL,
  PRIMARY KEY(zlibrary_id)
);
LOAD DATA INFILE '/var/lib/mysql/ipfs.csv'
  INTO TABLE zlib_ipfs
  FIELDS TERMINATED BY ',';
```

## Library Genesis ".rs-fork"

Get `libgen.rar` and `fiction.rar` from http://libgen.rs/dbdumps/ and extract them.

```bash
pv libgen.sql | ./run mysql allthethings
pv fiction.sql | ./run mysql allthethings
```

```sql
DROP TRIGGER libgen_description_update_all;
DROP TRIGGER libgen_updated_update_all;

ALTER TABLE updated RENAME libgenrs_updated;
ALTER TABLE description RENAME libgenrs_description;
ALTER TABLE hashes RENAME libgenrs_hashes;

ALTER TABLE fiction RENAME libgenrs_fiction;
ALTER TABLE fiction_description RENAME libgenrs_fiction_description;
ALTER TABLE fiction_hashes RENAME libgenrs_fiction_hashes;

ALTER TABLE libgenrs_hashes ADD PRIMARY KEY(md5);

ALTER TABLE topics RENAME libgenrs_topics;

SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION';
ALTER TABLE libgenrs_description DROP INDEX `time`;
ALTER TABLE libgenrs_hashes DROP INDEX `MD5`; -- Redundant with primary key.
ALTER TABLE libgenrs_updated DROP INDEX `Generic`, DROP INDEX `VisibleTimeAdded`, DROP INDEX `TimeAdded`, DROP INDEX `Topic`, DROP INDEX `VisibleID`, DROP INDEX `VisibleTimeLastModified`, DROP INDEX `TimeLastModifiedID`, DROP INDEX `DOI_INDEX`, DROP INDEX `Identifier`, DROP INDEX `Language`, DROP INDEX `Title`, DROP INDEX `Author`, DROP INDEX `Language_FTS`, DROP INDEX `Extension`, DROP INDEX `Publisher`, DROP INDEX `Series`, DROP INDEX `Year`, DROP INDEX `Title1`, DROP INDEX `Tags`, DROP INDEX `Identifierfulltext`;
ALTER TABLE libgenrs_fiction DROP INDEX `Language`, DROP INDEX `TITLE`, DROP INDEX `Authors`, DROP INDEX `Series`, DROP INDEX `Title+Authors+Series`, DROP INDEX `Identifier`;
```

## Library Genesis ".li-fork"

Download and extract the MyISAM tables from https://libgen.li/dirlist.php?dir=dbdumps.

Somehow load them into MariaDB. When I first did this I couldn't figure out how to do this with the latest MyISAM, so I used an older MySQL version, and then exported and imported. But surely we can figure out an easier way..

```sql
# Used this to generate this list: SELECT Concat('DROP TRIGGER ', Trigger_Name, ';') FROM  information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = 'libgen_new';
# (from https://stackoverflow.com/a/30339930)
DROP TRIGGER authors_before_ins_tr;
DROP TRIGGER authors_add_descr_before_ins_tr;
DROP TRIGGER authors_add_descr_before_upd_tr;
DROP TRIGGER authors_add_descr_before_del_tr1;
DROP TRIGGER editions_before_ins_tr1;
DROP TRIGGER editions_before_upd_tr1;
DROP TRIGGER editions_before_del_tr1;
DROP TRIGGER editions_add_descr_before_ins_tr;
DROP TRIGGER editions_add_descr_after_ins_tr;
DROP TRIGGER editions_add_descr_before_upd_tr;
DROP TRIGGER editions_add_descr_after_upd_tr;
DROP TRIGGER editions_add_descr_before_del_tr;
DROP TRIGGER editions_add_descr_after_del_tr;
DROP TRIGGER editions_to_files_before_ins_tr;
DROP TRIGGER editions_to_files_before_upd_tr;
DROP TRIGGER editions_to_files_before_del_tr;
DROP TRIGGER files_before_ins_tr;
DROP TRIGGER files_before_upd_tr;
DROP TRIGGER files_before_del_tr;
DROP TRIGGER files_add_descr_before_ins_tr;
DROP TRIGGER files_add_descr_before_upd_tr;
DROP TRIGGER files_add_descr_before_del_tr1;
DROP TRIGGER publisher_before_ins_tr;
DROP TRIGGER publisher_before_upd_tr;
DROP TRIGGER publisher_before_del_tr;
DROP TRIGGER publisher_add_descr_before_ins_tr;
DROP TRIGGER publisher_add_descr_before_upd_tr;
DROP TRIGGER publisher_add_descr_before_del_tr;
DROP TRIGGER series_before_ins_tr;
DROP TRIGGER series_before_upd_tr;
DROP TRIGGER series_before_del_tr;
DROP TRIGGER series_add_descr_before_ins_tr;
DROP TRIGGER series_add_descr_after_ins_tr;
DROP TRIGGER series_add_descr_before_upd_tr;
DROP TRIGGER series_add_descr_after_upd_tr;
DROP TRIGGER series_add_descr_before_del_tr;
DROP TRIGGER series_add_descr_after_del_tr;
DROP TRIGGER works_before_ins_tr;
DROP TRIGGER works_before_upd_tr;
DROP TRIGGER works_before_del_tr;
DROP TRIGGER works_add_descr_before_ins_tr;
DROP TRIGGER works_add_descr_before_upd_tr;
DROP TRIGGER works_add_descr_before_del_tr;
DROP TRIGGER works_to_editions_before_ins_tr;
DROP TRIGGER works_to_editions_before_upd_tr;
DROP TRIGGER works_to_editions_before_del_tr;

ALTER TABLE libgen_new.elem_descr RENAME allthethings.libgenli_elem_descr;
ALTER TABLE libgen_new.files RENAME allthethings.libgenli_files;
ALTER TABLE libgen_new.editions RENAME allthethings.libgenli_editions;
ALTER TABLE libgen_new.editions_to_files RENAME allthethings.libgenli_editions_to_files;
ALTER TABLE libgen_new.editions_add_descr RENAME allthethings.libgenli_editions_add_descr;
ALTER TABLE libgen_new.files_add_descr RENAME allthethings.libgenli_files_add_descr;
ALTER TABLE libgen_new.series RENAME allthethings.libgenli_series;
ALTER TABLE libgen_new.series_add_descr RENAME allthethings.libgenli_series_add_descr;
ALTER TABLE libgen_new.publishers RENAME allthethings.libgenli_publishers;

SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION';
ALTER TABLE libgenli_editions DROP INDEX `YEAR`, DROP INDEX `N_YEAR`, DROP INDEX `MONTH`, DROP INDEX `MONTH_END`, DROP INDEX `VISIBLE`, DROP INDEX `LG_TOP`, DROP INDEX `TYPE`, DROP INDEX `COMMENT`, DROP INDEX `S_ID`, DROP INDEX `DOI`, DROP INDEX `ISSUE`, DROP INDEX `DAY`, DROP INDEX `TIME`, DROP INDEX `TIMELM`;
ALTER TABLE libgenli_editions_add_descr DROP INDEX `TIME`, DROP INDEX `VAL3`, DROP INDEX `VAL`, DROP INDEX `VAL2`, DROP INDEX `VAL1`, DROP INDEX `VAL_ID`, DROP INDEX `VAL_UNIQ`, DROP INDEX `KEY`;
ALTER TABLE libgenli_editions_to_files DROP INDEX `TIME`, DROP INDEX `FID`; -- f_id is already covered by `IDS`.
ALTER TABLE libgenli_elem_descr DROP INDEX `key`;
ALTER TABLE libgenli_files DROP INDEX `md5_2`, DROP INDEX `MAGZID`, DROP INDEX `COMICSID`, DROP INDEX `LGTOPIC`, DROP INDEX `FICID`, DROP INDEX `FICTRID`, DROP INDEX `SMID`, DROP INDEX `STDID`, DROP INDEX `LGID`, DROP INDEX `FSIZE`, DROP INDEX `SMPATH`, DROP INDEX `TIME`, DROP INDEX `TIMELM`;
ALTER TABLE libgenli_files_add_descr DROP INDEX `TIME`, DROP INDEX `VAL`, DROP INDEX `KEY`;
ALTER TABLE libgenli_publishers DROP INDEX `TIME`, DROP INDEX `COM`, DROP INDEX `FULLTEXT`;
ALTER TABLE libgenli_series DROP INDEX `LG_TOP`, DROP INDEX `TIME`, DROP INDEX `TYPE`, DROP INDEX `VISIBLE`, DROP INDEX `COMMENT`, DROP INDEX `VAL_FULLTEXT`;
ALTER TABLE libgenli_series_add_descr DROP INDEX `TIME`, DROP INDEX `VAL`, DROP INDEX `VAL1`, DROP INDEX `VAL2`, DROP INDEX `VAL3`;
```

## Open Library

```bash
wget https://openlibrary.org/data/ol_dump_latest.txt.gz

gzip -d ol_dump_latest.txt.gz
```

```sql
CREATE TABLE ol_base (
    type CHAR(40) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
    ol_key CHAR(250) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
    revision INTEGER NOT NULL,
    last_modified DATETIME NOT NULL,
    json JSON NOT NULL
) ENGINE=MyISAM;
```

```bash
pv ol_dump_latest.txt | sed -e 's/\\u0000//g' | ./run mysql allthethings --local-infile=1 --show-warnings -vv -e "TRUNCATE ol_base; LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE ol_base FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '';"
```

```sql
SET SESSION myisam_sort_buffer_size = 75*1024*1024*1024;

-- ~37 mins
ALTER TABLE ol_base ADD PRIMARY KEY(ol_key);

-- ~20mins
CREATE TABLE ol_isbn13 (PRIMARY KEY(isbn, ol_key)) ENGINE=MyISAM IGNORE SELECT x.isbn AS isbn, ol_key FROM ol_base b CROSS JOIN JSON_TABLE(b.json, '$.isbn_13[*]' COLUMNS (isbn CHAR(13) PATH '$')) x WHERE ol_key LIKE '/books/OL%';
```

## ISBNdb

Download `isbndb_2022_09.jsonl.gz` from pilimi.org. 

```sql
CREATE TABLE `isbndb_isbns` (
  `isbn13` char(13) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
  `isbn10` char(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL,
  `json` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`json`)),
  PRIMARY KEY (`isbn13`,`isbn10`),
  KEY `isbn10` (`isbn10`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

TODO: figure out how to best load this.
