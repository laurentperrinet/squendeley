from sqlalchemy.ext.sqlsoup import SqlSoup
from sqlalchemy import create_engine,MetaData
from sqlalchemy import Table,Column,Integer,String,ForeignKey
from sqlalchemy.sql import and_
from sqlalchemy.orm import sessionmaker

import sqlite3
import os

import re
import lxml.etree
from lxml.builder import ElementMaker

__author__ = 'John C. McCullough'
__version__ = '0.1'

class Squendeley(object):
    def __init__(self, path=None, user=None):
        """
        Sqlite does not enforce column types and Mendeley's ORM does not
        convert "true"/"false" into the integer equivalents.  SqlAlchemy
        fails if there are column type violations.  On on successful
        load, the database is modified to convert "true"/"false" into
        1/0, which Mendeley seems to accept.

        Keyword arguments:
        path -- the path to your mendeley sqlite database.
                Something like /home/you/.local/share/data/Mendeley
                Ltd./Mendeley Desktop/you@you.com@www.mendeley.com.sqlite
                Will attempt to automatically derive path from username
                if unspecified.
        user -- your mendeley account user name, only necessary to
                construct a path
        """

        if path is None:
            if user is None:
                raise Exception("Must specify path or user")
            else:
                import platform
                system = platform.system()

                if system == 'Linux':
                    self.path = os.path.expanduser(
                                    os.path.join(
                                            '~',
                                            '.local',
                                            'share',
                                            'data',
                                            'Mendeley Ltd.',
                                            'Mendeley Desktop',
                                            '%s@www.mendeley.com.sqlite' % user
                                                 )
                                             )
                else:
                    raise Exception("Path-structure unknown for %s" % system)

                if not os.path.exists(self.path):
                    raise Exception("Couldn't locate mendeley db by user: "\
                                    + user)

        self._fixup(self.path)
        self._setup_sqlalchemy(self.path)

        self.E = ElementMaker(namespace="e", nsmap={"e":"e"})

    def _fixup(self, path):
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row

        c = conn.cursor()

        master = c.execute("select * from sqlite_master where type='table'")\
                  .fetchall()

        def extract_type(col):
            try:
                val = col.split()[1]
                if val == "NULL": return "VARCHAR"
                else: return val
            except:
                return "VARCHAR"

        def extract_name(col):
            val = col.split()[0]
            return val

        table_types = {}
        table_cols = {}
        for m in master:
            table = m[1]
            col_str = m[4][m[4].index('(')+1:-1]
            cols = col_str.split(",")
            types = [extract_type(col.strip()) for col in cols]
            names = [extract_name(col.strip()) for col in cols]
            table_types[table] = types
            table_cols[table] = names

        for table in table_types.keys():
            types = table_types[table]
            names = table_cols[table]
            rows = c.execute("select * from %s" % table).fetchall()
            bad_cols = {}
            for row in rows:
                for i in xrange(len(types)):
                    if types[i].startswith('INT') or types[i] == 'BOOL':
                        try:
                            a = int(row[i])
                        except:
                            if row[i] is not None:
                                vals = bad_cols.get(i, set())
                                vals.add(row[i])
                                bad_cols[i] = vals

            replace_int = {
                            "true" : '1',
                            "false" : '0',
                          }
            if len(bad_cols):
                for i in bad_cols.keys():
                    print i, bad_cols[i], names[i]

                    for val in bad_cols[i]:
                        if replace_int.has_key(val):
                            sql = "update %s set %s=%s where %s='%s'" % \
                                (table, names[i], replace_int[val],
                                        names[i], val)
                            print sql
                            c.execute(sql)


        conn.commit()
        conn.close()


    def _setup_sqlalchemy(self, path):
        """Establish foreign key relationships in the database and
        fire-up sqlsoup"""
        Session = sessionmaker()
        self.engine = create_engine('sqlite:///%s' % path)
        self.meta = MetaData(self.engine)
        self.connection = self.engine.connect()
        self.session = Session(bind=self.connection)

        Groups = Table('Groups', self.meta, autoload=True)

        Documents = Table('Documents', self.meta, autoload=True)

        RemoteDocuments = Table('RemoteDocuments', self.meta,
                                Column('groupId', Integer,
                                       ForeignKey('Groups.id')),
                                Column('remoteId', Integer,
                                       primary_key=True),
                                Column('documentId', Integer,
                                       ForeignKey('Documents.id'),
                                       primary_key=True),
                                autoload=True)

        DocumentContributors = Table('DocumentContributors', self.meta,
                                     Column('documentId', Integer,
                                            ForeignKey('Documents.id')),
                                     autoload=True)

        DocumentUrls = Table('DocumentUrls', self.meta,
                             Column('documentId', Integer,
                                    ForeignKey('Documents.id'),
                                    primary_key=True),
                             autoload=True)

        DocumentFolders = Table('DocumentFolders', self.meta,
                            Column('documentId', Integer,
                                   ForeignKey('Documents.id'),
                                   primary_key=True),
                            Column('folderId', Integer,
                                   ForeignKey('Folders.id'),
                                   primary_key=True),
                            autoload=True)

        DocumentTags = Table('DocumentTags', self.meta,
                            Column('documentId', Integer,
                                   ForeignKey('Documents.id'),
                                   primary_key=True),
                            Column('tag', String,
                                    primary_key=True),
                            autoload=True)

        Folders = Table('Folders', self.meta,
                        autoload=True)

        self.db = SqlSoup(self.meta)

        self.db.Documents.relate('contributors', self.db.DocumentContributors)
        self.db.Documents.relate('url', self.db.DocumentUrls)
        self.db.Documents.relate('tags', self.db.DocumentTags)

        self.f_doc_alive = and_(self.db.Documents.onlyReference != 1,
                                self.db.Documents.deletionPending != 1)

    def DocumentsFromSharedCollection(self, collection_name):
        g = self.db.Groups.filter(self.db.Groups.name==collection_name)
        rd = self.db.RemoteDocuments.join(g)
        d = self.db.Documents.join(rd.subquery())\
                        .filter(self.f_doc_alive)
        return d

    def DocumentsFromFolder(self, folder_name):
        f = self.db.Folders.filter(self.db.Folders.name==folder_name)
        fd = self.db.DocumentFolders.join(f)
        d = self.db.Documents.join(fd.subquery())\
                             .filter(self.f_doc_alive)

        return d

    def unescape(self, string):
        handle_entities = re.compile("&(\w+);").sub
        linebreak_re = re.compile('<m:linebreak/>')
        note_re = re.compile('<[/]?m:note>')
        italics_re = re.compile('<m:italic>([^<]*)</m:italic>')

        def html_unescape(s):
            from htmlentitydefs import name2codepoint
            if not s: return ''
            def unescape_entity(m):
                try:
                    return unichr(name2codepoint[m.group(1)])
                except KeyError:
                    return m.group(0)
            return handle_entities(unescape_entity, s)

        string = re.sub(italics_re, lambda m: '*%s*' % m.group(1), string)
        string = re.sub(note_re, '', re.sub(linebreak_re, '\n', string))
        return html_unescape(string)

    def escape(self, string):
        linebreak_re = re.compile('[\r\n]{1,2}')
        split_string = re.split(linebreak_re, string)

        interposed_string = [split_string[0]]
        for i in range(1,len(split_string)):
            interposed_string.append(self.E.linebreak())
            interposed_string.append(split_string[i])

        doc = self.E.garbage(self.E.note(
                *interposed_string
                         ))
        # Using garbage to get the xmlns line and remove it
        return lxml.etree.tostring(doc)[23:-12]

# vim: set ts=4 sw=4 expandtab:
