#!/usr/bin/env python3
#
# Copyright (c) 2017-2024 Colin Perkins
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
#2. Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from datetime import datetime, timedelta
from typing   import NewType, Iterator, List, Optional, Tuple, Dict
from pathlib  import Path

import xml.etree.ElementTree as ET
import requests
import os
import sys
import sqlite3
import time

# ==================================================================================================
# Helper classes for reading and parsing the RFC Index

DocID = NewType('DocID', str)

class RfcEntry:
    """
    An RFC entry in the rfc-index.xml file. No attempt is made to
    normalise the data included here.
    """
    doc_id       : DocID                # DocumentID (e.g., "RFC8700")
    title        : str
    authors      : List[str]
    doi          : str
    stream       : str
    wg           : Optional[str]        # For IETF stream RFCs, the working group
    area         : Optional[str]        # For IETF stream RFCs, the area
    publ_status  : str                  # The RFC status when published
    curr_status  : str                  # The RFC status now
    day          : Optional[int]        # The publication day; only recorded for 1 April RFCs
    month        : str                  # The publication month (e.g., "December")
    year         : int                  # The publication year
    formats      : List[str]
    draft        : Optional[str]        # The Internet-draft that became this RFC
    keywords     : List[str]
    updates      : List[DocID]
    updated_by   : List[DocID]
    obsoletes    : List[DocID]
    obsoleted_by : List[DocID]
    is_also      : List[DocID]
    see_also     : List[DocID]
    errata_url   : Optional[str]
    abstract     : Optional[ET.Element] # The abstract, as formatted XML
    page_count   : int


    def __init__(self, rfc_element: ET.Element) -> None:
        self.wg           = None
        self.area         = None
        self.day          = None
        self.errata_url   = None
        self.abstract     = None
        self.draft        = None
        self.authors      = []
        self.keywords     = []
        self.updates      = []
        self.updated_by   = []
        self.obsoletes    = []
        self.obsoleted_by = []
        self.is_also      = []
        self.see_also     = []
        self.formats      = []

        for elem in rfc_element:
            if   elem.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                assert elem.text is not None
                self.doc_id = DocID(elem.text)
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}title":
                assert elem.text is not None
                self.title  = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}doi":
                assert elem.text is not None
                self.doi = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}stream":
                assert elem.text is not None
                self.stream = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}wg_acronym":
                self.wg = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}area":
                self.area = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}current-status":
                assert elem.text is not None
                self.curr_status = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}publication-status":
                assert elem.text is not None
                self.publ_status = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}author":
                for inner in elem:
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}name":
                        assert inner.text is not None
                        self.authors.append(inner.text)
                    elif inner.tag == "{http://www.rfc-editor.org/rfc-index}title":
                        # Ignore <title>...</title> within <author>...</author> tags
                        # (this is normally just "Editor", which isn't useful)
                        pass 
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}date":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}day":
                        # <day>...</day> is only included for 1 April RFCs
                        self.day = int(inner.text)
                    elif inner.tag == "{http://www.rfc-editor.org/rfc-index}month":
                        self.month = inner.text
                    elif inner.tag == "{http://www.rfc-editor.org/rfc-index}year":
                        self.year = int(inner.text)
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}format":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}file-format":
                        self.formats.append(inner.text)
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}draft":
                if elem.text == "rfc4049bis":
                    # RFC 6019 is RFC 4049 republished as a Proposed Standard RF. 
                    # with virtually no change. It was never published as a draft,
                    # but the index lists "rfc4049bis" as its draft name. Replace
                    # this with the name of the draft that became RFC 4049.
                    self.draft = "draft-housley-binarytime-02"
                elif elem.text == "draft-luckie-recn":
                    self.draft = None
                else:
                    self.draft = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}keywords":
                for inner in elem:
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}kw":
                        # Omit empty <kw></kw> 
                        if inner.text is not None:
                            self.keywords.append(inner.text)
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}updates":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.updates.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}updated-by":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.updated_by.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}obsoletes":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.obsoletes.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}obsoleted-by":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.obsoleted_by.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}is-also":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.is_also.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}see-also":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.see_also.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}errata-url":
                self.errata_url = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}abstract":
                # The <abstract>...</abstract> contains formatted XML, most
                # typically a sequence of <p>...</p> tags.
                self.abstract = elem
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}page-count":
                assert elem.text is not None
                self.page_count = int(elem.text)
            else:
                print("Unknown tag: " + elem.tag)
                raise NotImplementedError


    def __str__(self) -> str:
        return "RFC {\n" \
             + "      doc_id: " + self.doc_id            + "\n" \
             + "       title: " + self.title             + "\n" \
             + "     authors: " + str(self.authors)      + "\n" \
             + "         doi: " + self.doi               + "\n" \
             + "      stream: " + self.stream            + "\n" \
             + "          wg: " + str(self.wg)           + "\n" \
             + "        area: " + str(self.area)         + "\n" \
             + " curr_status: " + self.curr_status       + "\n" \
             + " publ_status: " + self.publ_status       + "\n" \
             + "         day: " + str(self.day)          + "\n" \
             + "       month: " + self.month             + "\n" \
             + "        year: " + str(self.year)         + "\n" \
             + "     formats: " + str(self.formats)      + "\n" \
             + "       draft: " + str(self.draft)        + "\n" \
             + "    keywords: " + str(self.keywords)     + "\n" \
             + "     updates: " + str(self.updates)      + "\n" \
             + "  updated_by: " + str(self.updated_by)   + "\n" \
             + "   obsoletes: " + str(self.obsoletes)    + "\n" \
             + "obsoleted_by: " + str(self.obsoleted_by) + "\n" \
             + "     is_also: " + str(self.is_also)      + "\n" \
             + "    see_also: " + str(self.see_also)     + "\n" \
             + "  errata_url: " + str(self.errata_url)   + "\n" \
             + "    abstract: " + str(self.abstract)     + "\n" \
             + "}\n"


    def charset(self) -> str:
        """
        Most RFCs are UTF-8, or it's ASCII subset. A few are not. Return
        an appropriate encoding for the text of this RFC.
        """
        if   (self.doc_id == "RFC0064") or (self.doc_id == "RFC0101") or \
             (self.doc_id == "RFC0177") or (self.doc_id == "RFC0178") or \
             (self.doc_id == "RFC0182") or (self.doc_id == "RFC0227") or \
             (self.doc_id == "RFC0234") or (self.doc_id == "RFC0235") or \
             (self.doc_id == "RFC0237") or (self.doc_id == "RFC0243") or \
             (self.doc_id == "RFC0270") or (self.doc_id == "RFC0282") or \
             (self.doc_id == "RFC0288") or (self.doc_id == "RFC0290") or \
             (self.doc_id == "RFC0292") or (self.doc_id == "RFC0303") or \
             (self.doc_id == "RFC0306") or (self.doc_id == "RFC0307") or \
             (self.doc_id == "RFC0310") or (self.doc_id == "RFC0313") or \
             (self.doc_id == "RFC0315") or (self.doc_id == "RFC0316") or \
             (self.doc_id == "RFC0317") or (self.doc_id == "RFC0323") or \
             (self.doc_id == "RFC0327") or (self.doc_id == "RFC0367") or \
             (self.doc_id == "RFC0369") or (self.doc_id == "RFC0441") or \
             (self.doc_id == "RFC1305"):
            return "iso8859_1"
        elif self.doc_id == "RFC2166":
            return "windows-1252"
        elif (self.doc_id == "RFC2497") or (self.doc_id == "RFC2557"):
            return "iso8859_1"
        elif self.doc_id == "RFC2708":
            # This RFC is corrupt: line 521 has a byte with value 0xC6 that
            # is clearly intended to be a ' character, but that code point
            # doesn't correspond to ' in any character set I can find. Use
            # ISO 8859-1 which gets all characters right apart from this.
            #
            # According to Greg Skinner: "regarding the test in line 268
            # for RFC2708, as far as I can tell, U+0092 was introduced in
            # draft-ietf-printmib-job-protomap-01 in multiple places. In -02,
            # it was replaced with U+0027 everywhere except section 5.0.
            # Somehow, that stray character became the corrupt text you
            # identified."
            # (https://github.com/glasgow-ipl/ietfdata/issues/137)
            return "iso8859_1"
        elif self.doc_id == "RFC2875":
            # Both the text and PDF versions of this document have corrupt
            # characters (lines 754 and 926 of the text version). Using 
            # ISO 8859-1 is no more corrupt than the original.
            return "iso8859_1"
        else:
            return "utf-8"


    def content_url(self, required_format: str) -> Optional[str]:
        rfcnum = "rfc" + self.doc_id[3:].lstrip("0")
        for fmt in self.formats:
            if fmt == required_format:
                if required_format in [ "ASCII", "TEXT"] :
                    return "https://www.rfc-editor.org/rfc/" + rfcnum + ".txt"
                elif required_format == "PS":
                    return "https://www.rfc-editor.org/rfc/" + rfcnum + ".ps"
                elif required_format == "PDF":
                    return "https://www.rfc-editor.org/rfc/" + rfcnum + ".pdf"
                elif required_format == "HTML":
                    return "https://www.rfc-editor.org/rfc/" + rfcnum + ".html"
                elif required_format == "XML":
                    return "https://www.rfc-editor.org/rfc/" + rfcnum + ".xml"
                else:
                    return None
        return None


    def date(self) -> datetime:
        if self.day != None:
            date = "{} {} {}".format(self.day, self.month, self.year)
            return datetime.strptime(date, "%d %B %Y")
        else:
            date = "{} {}".format(self.month, self.year)
            return datetime.strptime(date, "%B %Y")



# ==================================================================================================

class RfcNotIssuedEntry:
    """
      An RFC that was not issued in the rfc-index.xml file.
    """
    doc_id : DocID


    def __init__(self, rfc_not_issued_element: ET.Element) -> None:
        for elem in rfc_not_issued_element:
            if   elem.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                assert elem.text is not None
                self.doc_id = DocID(elem.text)
            else:
                raise NotImplementedError


    def __str__(self) -> str:
        return "RFC-Not-Issued {\n" \
             + "      doc_id: " + self.doc_id + "\n" \
             + "}\n"


# ==================================================================================================

class BcpEntry:
    """
      A BCP entry in the rfc-index.xml file.
    """
    doc_id  : DocID
    is_also : List[DocID]


    def __init__(self, bcp_element: ET.Element) -> None:
        self.is_also = []

        for elem in bcp_element:
            if   elem.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                assert elem.text is not None
                self.doc_id = DocID(elem.text)
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}is-also":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.is_also.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            else:
                raise NotImplementedError


    def __str__(self) -> str:
        return "BCP {\n" \
             + "      doc_id: " + self.doc_id        + "\n" \
             + "     is_also: " + str(self.is_also)  + "\n" \
             + "}\n"


# ==================================================================================================

class StdEntry:
    """
      An STD entry in the rfc-index.xml file.
    """
    doc_id  : DocID
    title   : str
    is_also : List[DocID]


    def __init__(self, std_element: ET.Element) -> None:
        self.is_also = []

        for elem in std_element:
            assert elem.text is not None
            if   elem.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                self.doc_id = DocID(elem.text)
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}title":
                self.title  = elem.text
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}is-also":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.is_also.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            else:
                raise NotImplementedError


    def __str__(self) -> str:
        return "STD {\n" \
             + "      doc_id: " + self.doc_id       + "\n" \
             + "       title: " + self.title        + "\n" \
             + "     is_also: " + str(self.is_also) + "\n" \
             + "}\n"

# ==================================================================================================

class FyiEntry:
    """
      A FYI entry in the rfc-index.xml file.
    """
    doc_id   : DocID
    is_also  : List[DocID]


    def __init__(self, fyi_element: ET.Element) -> None:
        self.is_also = []

        for elem in fyi_element:
            assert elem.text is not None
            if   elem.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                self.doc_id = DocID(elem.text)
            elif elem.tag == "{http://www.rfc-editor.org/rfc-index}is-also":
                for inner in elem:
                    assert inner.text is not None
                    if   inner.tag == "{http://www.rfc-editor.org/rfc-index}doc-id":
                        self.is_also.append(DocID(inner.text))
                    else:
                        raise NotImplementedError
            else:
                raise NotImplementedError


    def __str__(self) -> str:
        return "FYI {\n" \
             + "      doc_id: " + self.doc_id       + "\n" \
             + "     is_also: " + str(self.is_also) + "\n" \
             + "}\n"


# ==================================================================================================

class RFCIndex:
    """
    The RFC Index.
    """
    _rfc            : Dict[str, RfcEntry]
    _rfc_not_issued : Dict[str, RfcNotIssuedEntry]
    _bcp            : Dict[str, BcpEntry]
    _std            : Dict[str, StdEntry]
    _fyi            : Dict[str, FyiEntry]


    def _download_index(self) -> Optional[str]:
        with requests.Session() as session:
            response = session.get("https://www.rfc-editor.org/rfc-index.xml", verify=True)
            if response.status_code == 200:
                return response.text
            else:
                return None


    def _is_cached(self, cache_filepath : Path) -> bool:
        if cache_filepath.exists():
            curr_time = datetime.now()
            prev_time = datetime.fromtimestamp(cache_filepath.stat().st_mtime)
            if curr_time < prev_time + timedelta(days = 1):
                return True
        return False


    def _retrieve_index(self) -> Optional[str]:
        if self.cache_dir is not None:
            cache_filepath = Path(self.cache_dir, "rfc", "rfc-index.xml")
            if self._is_cached(cache_filepath):
                with open(cache_filepath, "r") as cache_file:
                    return cache_file.read()
            else:
                response = self._download_index()
                if response is not None:
                    cache_filepath.parent.mkdir(parents=True, exist_ok=True)
                    with open(cache_filepath, "w") as cache_file:
                        cache_file.write(response)
                        return response
                else:
                    return None
        else:
            return self._download_index()


    def __init__(self, cache_dir: Optional[Path] = None):
        """
        Parameters:
            cache_dir      -- If set, use this directory as a cache for Datatracker objects
        """
        self.cache_dir       = cache_dir
        self._rfc            = {}
        self._rfc_not_issued = {}
        self._bcp            = {}
        self._std            = {}
        self._fyi            = {}

        xml = self._retrieve_index()
        if xml is None:
            raise RuntimeError

        for doc in ET.fromstring(xml):
            if   doc.tag == "{http://www.rfc-editor.org/rfc-index}rfc-entry":
                rfc = RfcEntry(doc)
                self._rfc[rfc.doc_id] = rfc
            elif doc.tag == "{http://www.rfc-editor.org/rfc-index}rfc-not-issued-entry":
                rne = RfcNotIssuedEntry(doc)
                self._rfc_not_issued[rne.doc_id] = rne
            elif doc.tag == "{http://www.rfc-editor.org/rfc-index}bcp-entry":
                bcp = BcpEntry(doc)
                self._bcp[bcp.doc_id] = bcp
            elif doc.tag == "{http://www.rfc-editor.org/rfc-index}std-entry":
                std = StdEntry(doc)
                self._std[std.doc_id] = std
            elif doc.tag == "{http://www.rfc-editor.org/rfc-index}fyi-entry":
                fyi = FyiEntry(doc)
                self._fyi[fyi.doc_id] = fyi
            else:
                raise NotImplementedError


    def rfc(self, rfc_id: str) -> Optional[RfcEntry]:
        return self._rfc[rfc_id]


    def rfcs_not_issued(self) -> Iterator[RfcNotIssuedEntry]:
        for rfc_id in self._rfc_not_issued:
            yield self._rfc_not_issued[rfc_id]


    def rfc_not_issued(self, rfc_id: str) -> Optional[RfcNotIssuedEntry]:
        return self._rfc_not_issued[rfc_id]


    def bcps(self) -> Iterator[BcpEntry]:
        for bcp_id in self._bcp:
            yield self._bcp[bcp_id]


    def bcp(self, bcp_id: str) -> Optional[BcpEntry]:
        return self._bcp[bcp_id]


    def fyis(self) -> Iterator[FyiEntry]:
        for fyi_id in self._fyi:
            yield self._fyi[fyi_id]


    def fyi(self, fyi_id: str) -> Optional[FyiEntry]:
        return self._fyi[fyi_id]


    def stds(self) -> Iterator[StdEntry]:
        for std_id in self._std:
            yield self._std[std_id]


    def std(self, std_id: str) -> Optional[StdEntry]:
        return self._std[std_id]


    def rfcs(self,
            since:  str = "1969-01",  # The first RFCs were published in 1969
            until:  str = "2038-01",
            stream: Optional[str] = None,
            area:   Optional[str] = None,
            wg:     Optional[str] = None,
            status: Optional[str] = None) -> Iterator[RfcEntry]:
        for rfc_id in self._rfc:
            rfc = self._rfc[rfc_id]
            if stream is not None and rfc.stream != stream:
                continue
            if area   is not None and rfc.area   != area:
                continue
            if wg     is not None and rfc.wg     != wg:
                continue
            if status is not None and rfc.curr_status != status:
                continue
            if rfc.date() < datetime.strptime(since, "%Y-%m"):
                continue
            if rfc.date() > datetime.strptime(until, "%Y-%m"):
                continue
            yield(rfc)


# ==================================================================================================
# Code to fetch the RFC Index and write to a database

if len(sys.argv) == 2:
    database_file = sys.argv[1]
else:
    print("Usage: scripts/db-from-rfc-index.py <database.db>")
    sys.exit(1)

db_connection = sqlite3.connect(database_file)
db_cursor = db_connection.cursor()


db_tables = list(map(lambda x : x[0], db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")))
has_dt_tables = True
for name in ["ietf_dt_name_streamname", "ietf_dt_group_group"]:
    if name not in db_tables:
        has_dt_tables = False

if has_dt_tables:
    print("Database has ietf_dt_* tables")


sql =  f"CREATE TABLE ietf_ri_rfc (\n"
sql += f"  doc_id         TEXT PRIMARY KEY,\n"
sql += f"  title          TEXT NOT NULL,\n"
sql += f"  doi            TEXT NOT NULL,\n"
sql += f"  stream         TEXT NOT NULL,\n"
sql += f"  wg             TEXT,\n"
sql += f"  area           TEXT,\n"
sql += f"  curr_status    TEXT,\n"
sql += f"  publ_status    TEXT,\n"
sql += f"  day            TEXT,\n"
sql += f"  month          TEXT,\n"
sql += f"  year           INTEGER,\n"
sql += f"  draft          TEXT,\n"
sql += f"  draft_document TEXT,\n"
sql += f"  errata_url     TEXT,\n"
sql += f"  is_also        TEXT,\n"
sql += f"  abstract       TEXT,\n"
if has_dt_tables:
    sql += f"  FOREIGN KEY (stream)         REFERENCES ietf_dt_name_streamname (slug),\n"
    sql += f"  FOREIGN KEY (wg)             REFERENCES ietf_dt_group_group (acronym),\n"
    sql += f"  FOREIGN KEY (area)           REFERENCES ietf_dt_group_group (acronym),\n"
    sql += f"  FOREIGN KEY (draft_document) REFERENCES ietf_dt_doc_document (name),\n"
sql += f"  FOREIGN KEY (is_also)            REFERENCES ietf_ri_subseries (subseries_doc_id)\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_rfc_authors (\n"
sql += f"  id     INTEGER PRIMARY KEY,\n"
sql += f"  doc_id TEXT,\n"
sql += f"  author TEXT,\n"
sql += f"  FOREIGN KEY (doc_id) REFERENCES ietf_ri_rfc (doc_id)\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_rfc_formats (\n"
sql += f"  id     INTEGER PRIMARY KEY,\n"
sql += f"  doc_id TEXT,\n"
sql += f"  format TEXT,\n"
sql += f"  FOREIGN KEY (doc_id) REFERENCES ietf_ri_rfc (doc_id)\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_rfc_keywords (\n"
sql += f"  id      INTEGER PRIMARY KEY,\n"
sql += f"  doc_id  TEXT,\n"
sql += f"  keyword  TEXT,\n"
sql += f"  FOREIGN KEY (doc_id) REFERENCES ietf_ri_rfc (doc_id)\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_rfc_relationships (\n"
sql += f"  id           INTEGER PRIMARY KEY,\n"
sql += f"  doc_id       TEXT,\n"
sql += f"  relationship TEXT,\n"
sql += f"  related_doc  TEXT,\n"
sql += f"  FOREIGN KEY (doc_id) REFERENCES ietf_ri_rfc (doc_id)\n"
sql += f"  FOREIGN KEY (related_doc) REFERENCES ietf_ri_rfc (doc_id)\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_rfcnotissued (\n"
sql += f"  id      INTEGER PRIMARY KEY,\n"
sql += f"  doc_id  TEXT\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_subseries (\n"
sql += f"  subseries_doc_id  TEXT PRIMARY KEY,\n"
sql += f"  is_bcp  INTEGER,\n"
sql += f"  is_fyi  INTEGER,\n"
sql += f"  is_std  INTEGER\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_bcp (\n"
sql += f"  id      INTEGER PRIMARY KEY,\n"
sql += f"  bcp_id  TEXT,\n"
sql += f"  doc_id  TEXT,\n"
sql += f"  FOREIGN KEY (bcp_id) REFERENCES ietf_ri_subseries (subseries_doc_id)\n"
sql += f"  FOREIGN KEY (doc_id) REFERENCES ietf_ri_rfc (doc_id)\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_fyi (\n"
sql += f"  id      INTEGER PRIMARY KEY,\n"
sql += f"  fyi_id  TEXT,\n"
sql += f"  doc_id  TEXT,\n"
sql += f"  FOREIGN KEY (fyi_id) REFERENCES ietf_ri_subseries (subseries_doc_id)\n"
sql += f"  FOREIGN KEY (doc_id) REFERENCES ietf_ri_rfc (doc_id)\n"
sql += ");\n"
db_cursor.execute(sql)

sql =  f"CREATE TABLE ietf_ri_std (\n"
sql += f"  id      INTEGER PRIMARY KEY,\n"
sql += f"  std_id  TEXT,\n"
sql += f"  doc_id  TEXT,\n"
sql += f"  FOREIGN KEY (std_id) REFERENCES ietf_ri_subseries (subseries_doc_id)\n"
sql += f"  FOREIGN KEY (doc_id) REFERENCES ietf_ri_rfc (doc_id)\n"
sql += ");\n"
db_cursor.execute(sql)

print("Fetching RFC Index")
ri = RFCIndex()

for rfc in ri.rfcs():
    if rfc.abstract is not None:
        abstract = "\n\n".join(ET.tostringlist(rfc.abstract, encoding="unicode", method="text"))
    else:
        abstract = None
    if len(rfc.is_also) > 0:
        is_also = rfc.is_also[0]
    else:
        is_also = None
    if rfc.draft is not None:
        document = rfc.draft[:-3]
    else:
        document = None
    val = (rfc.doc_id,
           rfc.title,
           rfc.doi,
           rfc.stream.lower(),
           rfc.wg,
           rfc.area,
           rfc.curr_status,
           rfc.publ_status,
           rfc.day,
           rfc.month,
           rfc.year,
           rfc.draft,
           document,
           rfc.errata_url,
           is_also,
           abstract)
    sql = f"INSERT INTO ietf_ri_rfc VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    db_cursor.execute(sql, val)

    for author in rfc.authors:
        val = (None, rfc.doc_id, author)
        sql = "INSERT INTO ietf_ri_rfc_authors VALUES (?, ?, ?)"
        db_cursor.execute(sql, val)

    for format in rfc.formats:
        val = (None, rfc.doc_id, format)
        sql = "INSERT INTO ietf_ri_rfc_formats VALUES (?, ?, ?)"
        db_cursor.execute(sql, val)

    for keyword in rfc.keywords:
        val = (None, rfc.doc_id, keyword)
        sql = "INSERT INTO ietf_ri_rfc_keywords VALUES (?, ?, ?)"
        db_cursor.execute(sql, val)

    for updates in rfc.updates:
        val = (None, rfc.doc_id, "updates", updates)
        sql = "INSERT INTO ietf_ri_rfc_relationships VALUES (?, ?, ?, ?)"
        db_cursor.execute(sql, val)

    for updated_by in rfc.updated_by:
        val = (None, rfc.doc_id, "updated_by", updated_by)
        sql = "INSERT INTO ietf_ri_rfc_relationships VALUES (?, ?, ?, ?)"
        db_cursor.execute(sql, val)

    for obsoletes in rfc.obsoletes:
        val = (None, rfc.doc_id, "obsoletes", obsoletes)
        sql = "INSERT INTO ietf_ri_rfc_relationships VALUES (?, ?, ?, ?)"
        db_cursor.execute(sql, val)

    for obsoleted_by in rfc.obsoleted_by:
        val = (None, rfc.doc_id, "obsoleted_by", obsoleted_by)
        sql = "INSERT INTO ietf_ri_rfc_relationships VALUES (?, ?, ?, ?)"
        db_cursor.execute(sql, val)

    for see_also in rfc.see_also:
        val = (None, rfc.doc_id, "see_also", see_also)
        sql = "INSERT INTO ietf_ri_rfc_relationships VALUES (?, ?, ?, ?)"
        db_cursor.execute(sql, val)

db_connection.commit()

for rfc_ni in ri.rfcs_not_issued():
    val = (None, rfc_ni.doc_id )
    sql = "INSERT INTO ietf_ri_rfcnotissued VALUES (?, ?)"
    db_cursor.execute(sql, val)
db_connection.commit()

for bcp in ri.bcps():
    db_cursor.execute("INSERT INTO ietf_ri_subseries VALUES (?, ?, ?, ?)", (bcp.doc_id, 1, 0, 0))
    for bcp_doc in bcp.is_also:
        val = (None, bcp.doc_id, bcp_doc)
        sql = "INSERT INTO ietf_ri_bcp VALUES (?, ?, ?)"
        db_cursor.execute(sql, val)
db_connection.commit()


for fyi in ri.fyis():
    db_cursor.execute("INSERT INTO ietf_ri_subseries VALUES (?, ?, ?, ?)", (fyi.doc_id, 0, 1, 0))
    for fyi_doc in fyi.is_also:
        val = (None, fyi.doc_id, fyi_doc)
        sql = "INSERT INTO ietf_ri_fyi VALUES (?, ?, ?)"
        db_cursor.execute(sql, val)
db_connection.commit()


for std in ri.stds():
    db_cursor.execute("INSERT INTO ietf_ri_subseries VALUES (?, ?, ?, ?)", (std.doc_id, 0, 0, 1))
    for std_doc in std.is_also:
        val = (None, std.doc_id, std_doc)
        sql = "INSERT INTO ietf_ri_std VALUES (?, ?, ?)"
        db_cursor.execute(sql, val)
db_connection.commit()


print("Vacuuming database")
db_connection.execute('VACUUM;')

