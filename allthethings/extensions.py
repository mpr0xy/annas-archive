from flask_debugtoolbar import DebugToolbarExtension
from flask_sqlalchemy import SQLAlchemy
from flask_static_digest import FlaskStaticDigest
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.ext.declarative import DeferredReflection
from flask_elasticsearch import FlaskElasticsearch

debug_toolbar = DebugToolbarExtension()
flask_static_digest = FlaskStaticDigest()
db = SQLAlchemy()
Base = declarative_base()
es = FlaskElasticsearch()

class Reflected(DeferredReflection):
    __abstract__ = True
    def to_dict(self):
        unloaded = db.inspect(self).unloaded
        return dict((col.name, getattr(self, col.name)) for col in self.__table__.columns if col.name not in unloaded)

class ZlibBook(Reflected, Base):
    __tablename__ = "zlib_book"
    isbns = relationship("ZlibIsbn", lazy="selectin")
    ipfs = relationship("ZlibIpfs", lazy="joined")
class ZlibIsbn(Reflected, Base):
    __tablename__ = "zlib_isbn"
    zlibrary_id = Column(Integer, ForeignKey("zlib_book.zlibrary_id"))
class ZlibIpfs(Reflected, Base):
    __tablename__ = "zlib_ipfs"
    zlibrary_id = Column(Integer, ForeignKey("zlib_book.zlibrary_id"), primary_key=True)

class IsbndbIsbns(Reflected, Base):
    __tablename__ = "isbndb_isbns"

class LibgenliFiles(Reflected, Base):
    __tablename__ = "libgenli_files"
    add_descrs = relationship("LibgenliFilesAddDescr", lazy="selectin")
    editions = relationship("LibgenliEditions", lazy="selectin", secondary="libgenli_editions_to_files")
class LibgenliFilesAddDescr(Reflected, Base):
    __tablename__ = "libgenli_files_add_descr"
    f_id = Column(Integer, ForeignKey("libgenli_files.f_id"))
class LibgenliEditionsToFiles(Reflected, Base):
    __tablename__ = "libgenli_editions_to_files"
    f_id = Column(Integer, ForeignKey("libgenli_files.f_id"))
    e_id = Column(Integer, ForeignKey("libgenli_editions.e_id"))
class LibgenliEditions(Reflected, Base):
    __tablename__ = "libgenli_editions"
    issue_s_id = Column(Integer, ForeignKey("libgenli_series.s_id"))
    series = relationship("LibgenliSeries", lazy="joined")
    add_descrs = relationship("LibgenliEditionsAddDescr", lazy="selectin")
class LibgenliEditionsAddDescr(Reflected, Base):
    __tablename__ = "libgenli_editions_add_descr"
    e_id = Column(Integer, ForeignKey("libgenli_editions.e_id"))
    publisher = relationship("LibgenliPublishers", lazy="joined", primaryjoin="(remote(LibgenliEditionsAddDescr.value) == foreign(LibgenliPublishers.p_id)) & (LibgenliEditionsAddDescr.key == 308)")
class LibgenliPublishers(Reflected, Base):
    __tablename__ = "libgenli_publishers"
class LibgenliSeries(Reflected, Base):
    __tablename__ = "libgenli_series"
    issn_add_descrs = relationship("LibgenliSeriesAddDescr", lazy="joined", primaryjoin="(LibgenliSeries.s_id == LibgenliSeriesAddDescr.s_id) & (LibgenliSeriesAddDescr.key == 501)")
class LibgenliSeriesAddDescr(Reflected, Base):
    __tablename__ = "libgenli_series_add_descr"
    s_id = Column(Integer, ForeignKey("libgenli_series.s_id"))
class LibgenliElemDescr(Reflected, Base):
    __tablename__ = "libgenli_elem_descr"

class LibgenrsDescription(Reflected, Base):
    __tablename__ = "libgenrs_description"
class LibgenrsHashes(Reflected, Base):
    __tablename__ = "libgenrs_hashes"
class LibgenrsTopics(Reflected, Base):
    __tablename__ = "libgenrs_topics"
class LibgenrsUpdated(Reflected, Base):
    __tablename__ = "libgenrs_updated"

class LibgenrsFiction(Reflected, Base):
    __tablename__ = "libgenrs_fiction"
class LibgenrsFictionDescription(Reflected, Base):
    __tablename__ = "libgenrs_fiction_description"
class LibgenrsFictionHashes(Reflected, Base):
    __tablename__ = "libgenrs_fiction_hashes"

class OlBase(Reflected, Base):
    __tablename__ = "ol_base"

class ComputedAllMd5s(Reflected, Base):
    __tablename__ = "computed_all_md5s"
class ComputedSearchMd5Objs(Reflected, Base):
    __tablename__ = "computed_search_md5_objs"

