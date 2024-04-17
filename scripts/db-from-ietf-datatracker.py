#!/usr/bin/env python3
#
# Copyright (c) 2023-2024 Colin Perkins
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

import datetime
import json
import os
import requests
import sqlite3
import sys

from graphlib import TopologicalSorter
from typing   import Any, Dict, Iterator
from pprint   import pprint
from pathlib  import Path

class Datatracker:
    session : requests.session
    dt_url  : str
    cache   : Dict[str,Any]

    def __init__(self, dt_url: str):
        self.session = requests.Session()
        self.dt_url  = dt_url
        self.cache   = {}


    def fetch_multi(self, uri: str) -> Iterator[Dict[Any, Any]]:
        while uri is not None:
            if uri in self.cache:
                meta, objs = self.cache[uri]
                for obj in objs:
                    yield obj
                uri = meta["next"]
            else:
                r = self.session.get(f"{self.dt_url}{uri}")
                if r.status_code == 200:
                    meta = r.json()['meta']
                    objs = r.json()['objects']
                    for obj in objs:
                        yield obj
                    self.cache[uri] = (meta, objs)
                    uri = meta["next"]
                else:
                    print(f"Cannot fetch: {r.status_code}")
                    sys.exit(1)


    def api_endpoints(self) -> Iterator[str]:
        r1 = self.session.get(f"{self.dt_url}api/v1/")
        if r1.status_code == 200:
            for category in r1.json().values():
                r2 = self.session.get(f"{self.dt_url.rstrip('/')}{category['list_endpoint']}")
                if r2.status_code == 200:
                    for endpoint in r2.json().values():
                        yield endpoint["list_endpoint"]
                else:
                    print(f"Cannot fetch secondary API endpoint: {r2.status_code} ({r2.url})")
                    sys.exit(1)
        else:
            print(f"Cannot fetch top-level API endpoints: {r1.status_code}")
            sys.exit(1)


    def schema_for_endpoint(self, api_endpoint:str):
        resp = self.session.get(f"{self.dt_url.rstrip('/')}{endpoint}schema/")
        if resp.status_code == 200:
            schema = resp.json()
            result = {
                "api_endpoint": api_endpoint,
                "table"       : "ietf_dt" + api_endpoint.replace("/", "_")[7:-1],
                "sort_by"     : None,
                "primary_key" : None,
                "columns"     : {}
            }
            if "ordering" in schema:
                result["sort_by"] = schema["ordering"][0]
            if "historical" in api_endpoint:
                result["sort_by"] = None
                print(f"Not sorting {api_endpoint}")
            for field_name in schema["fields"]:
                column = {}
                column["name"]    = field_name
                column["type"]    = schema["fields"][field_name]["type"]
                column["unique"]  = schema["fields"][field_name]["unique"]
                column["primary"] = schema["fields"][field_name]["primary_key"]
                if column["primary"]:
                    result["primary_key"] = field_name
                if column["type"] == "related":
                    column["type"] = schema["fields"][field_name]["related_type"]
                result["columns"][field_name] = column
            return result
        else:
            print(f"ERROR: {resp.status_code} ({resp.url})")
            sys.exit(1)


endpoints_to_mirror = {
    "/api/v1/community/communitylist/"             : {"mirror": False, "uri_col": None}, # Not clear what this is
    "/api/v1/community/emailsubscription/"         : {"mirror": False, "uri_col": None}, # Not clear what this is
    "/api/v1/community/searchrule/"                : {"mirror": False, "uri_col": None}, # Not clear what this is
    "/api/v1/dbtemplate/dbtemplate/"               : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/addedmessageevent/"               : {"mirror": False, "uri_col": None}, # Unused in the datatracker
    "/api/v1/doc/ballotdocevent/"                  : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/ballotpositiondocevent/"          : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/ballottype/"                      : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/bofreqeditordocevent/"            : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/bofreqresponsibledocevent/"       : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/consensusdocevent/"               : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/deletedevent/"                    : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/docevent/"                        : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/docextresource/"                  : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/dochistory/"                      : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/dochistoryauthor/"                : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/docreminder/"                     : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/document/"                        : {"mirror": True,  "uri_col": "name"},
    "/api/v1/doc/documentactionholder/"            : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/documentauthor/"                  : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/documenturl/"                     : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/editedauthorsdocevent/"           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/ianaexpertdocevent/"              : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/initialreviewdocevent/"           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/irsgballotdocevent/"              : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/lastcalldocevent/"                : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/newrevisiondocevent/"             : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/relateddochistory/"               : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/relateddocument/"                 : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/reviewassignmentdocevent/"        : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/reviewrequestdocevent/"           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/state/"                           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/statedocevent/"                   : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/statetype/"                       : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/doc/submissiondocevent/"              : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/telechatdocevent/"                : {"mirror": True,  "uri_col": "id"},
    "/api/v1/doc/writeupdocevent/"                 : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/appeal/"                        : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/appealartifact/"                : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/changestategroupevent/"         : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/group/"                         : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/groupevent/"                    : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/groupextresource/"              : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/groupfeatures/"                 : {"mirror": False, "uri_col": None},
    "/api/v1/group/grouphistory/"                  : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/groupmilestone/"                : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/groupmilestonehistory/"         : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/groupstatetransitions/"         : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/groupurl/"                      : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/milestonegroupevent/"           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/role/"                          : {"mirror": True,  "uri_col": "id"},
    "/api/v1/group/rolehistory/"                   : {"mirror": True,  "uri_col": "id"},
    "/api/v1/iesg/telechat/"                       : {"mirror": False, "uri_col": None}, # Not available in public API
    "/api/v1/iesg/telechatagendacontent/"          : {"mirror": False, "uri_col": None}, # Not available in public API
    "/api/v1/iesg/telechatagendaitem/"             : {"mirror": False, "uri_col": None}, # Not available in public API
    "/api/v1/iesg/telechatdate/"                   : {"mirror": True,  "uri_col": "id"},
    "/api/v1/ipr/genericiprdisclosure/"            : {"mirror": True,  "uri_col": "id"},
    "/api/v1/ipr/holderiprdisclosure/"             : {"mirror": True,  "uri_col": "id"},
    "/api/v1/ipr/iprdisclosurebase/"               : {"mirror": True,  "uri_col": "id"},
    "/api/v1/ipr/iprdocrel/"                       : {"mirror": True,  "uri_col": "id"},
    "/api/v1/ipr/iprevent/"                        : {"mirror": False, "uri_col": None}, # Unused in datatracker
    "/api/v1/ipr/legacymigrationiprevent/"         : {"mirror": False, "uri_col": None}, # Unused in datatracker
    "/api/v1/ipr/nondocspecificiprdisclosure/"     : {"mirror": True,  "uri_col": "id"},
    "/api/v1/ipr/relatedipr/"                      : {"mirror": True,  "uri_col": "id"},
    "/api/v1/ipr/thirdpartyiprdisclosure/"         : {"mirror": True,  "uri_col": "id"},
    "/api/v1/liaisons/liaisonstatement/"           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/liaisons/liaisonstatementattachment/" : {"mirror": True,  "uri_col": "id"},
    "/api/v1/liaisons/liaisonstatementevent/"      : {"mirror": True,  "uri_col": "id"},
    "/api/v1/liaisons/relatedliaisonstatement/"    : {"mirror": True,  "uri_col": "id"},
    "/api/v1/mailinglists/allowlisted/"            : {"mirror": False, "uri_col": None}, # Not available in public API
    "/api/v1/mailinglists/list/"                   : {"mirror": False, "uri_col": None}, # Not available in public API
    "/api/v1/mailinglists/nonwgmailinglist/"       : {"mirror": False, "uri_col": None}, # Not available in public API
    "/api/v1/mailinglists/subscribed/"             : {"mirror": False, "uri_col": None}, # Not available in public API
    "/api/v1/mailtrigger/mailtrigger/"             : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/mailtrigger/recipient/"               : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/meeting/attended/"                    : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/businessconstraint/"          : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/meeting/constraint/"                  : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/floorplan/"                   : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/importantdate/"               : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/meeting/"                     : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/meetinghost/"                 : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/proceedingsmaterial/"         : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/resourceassociation/"         : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/room/"                        : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/schedtimesessassignment/"     : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/schedule/"                    : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/schedulingevent/"             : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/session/"                     : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/sessionpresentation/"         : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/slidesubmission/"             : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/timeslot/"                    : {"mirror": True,  "uri_col": "id"},
    "/api/v1/meeting/urlresource/"                 : {"mirror": True,  "uri_col": "id"},
    "/api/v1/message/announcementfrom/"            : {"mirror": False, "uri_col": None}, # Not useful with other messages unavailable
    "/api/v1/message/message/"                     : {"mirror": False, "uri_col": None}, # No longer available
    "/api/v1/message/messageattachment/"           : {"mirror": False, "uri_col": None}, # No longer available
    "/api/v1/message/sendqueue/"                   : {"mirror": False, "uri_col": None}, # No longer available
    "/api/v1/name/agendafiltertypename/"           : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/agendatypename/"                 : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/appealartifacttypename/"         : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/ballotpositionname/"             : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/constraintname/"                 : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/continentname/"                  : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/countryname/"                    : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/dbtemplatetypename/"             : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/docrelationshipname/"            : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/docremindertypename/"            : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/doctagname/"                     : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/doctypename/"                    : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/docurltagname/"                  : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/draftsubmissionstatename/"       : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/extresourcename/"                : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/extresourcetypename/"            : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/feedbacktypename/"               : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/formallanguagename/"             : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/groupmilestonestatename/"        : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/groupstatename/"                 : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/grouptypename/"                  : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/importantdatename/"              : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/intendedstdlevelname/"           : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/iprdisclosurestatename/"         : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/ipreventtypename/"               : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/iprlicensetypename/"             : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/liaisonstatementeventtypename/"  : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/liaisonstatementpurposename/"    : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/liaisonstatementstate/"          : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/liaisonstatementtagname/"        : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/meetingtypename/"                : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/nomineepositionstatename/"       : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/proceedingsmaterialtypename/"    : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/reviewassignmentstatename/"      : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/reviewerqueuepolicyname/"        : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/reviewrequeststatename/"         : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/reviewresultname/"               : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/reviewtypename/"                 : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/rolename/"                       : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/roomresourcename/"               : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/sessionpurposename/"             : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/sessionstatusname/"              : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/slidesubmissionstatusname/"      : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/stdlevelname/"                   : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/streamname/"                     : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/telechatagendasectionname/"      : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/timerangename/"                  : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/timeslottypename/"               : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/name/topicaudiencename/"              : {"mirror": True,  "uri_col": "slug"},
    "/api/v1/nomcom/feedback/"                     : {"mirror": False, "uri_col": None}, # Not available in the public API
    "/api/v1/nomcom/feedbacklastseen/"             : {"mirror": False, "uri_col": None}, # Not useful
    "/api/v1/nomcom/nomcom/"                       : {"mirror": False, "uri_col": None},
    "/api/v1/nomcom/nomination/"                   : {"mirror": True,  "uri_col": "id"},
    "/api/v1/nomcom/nominee/"                      : {"mirror": True,  "uri_col": "id"},
    "/api/v1/nomcom/nomineeposition/"              : {"mirror": True,  "uri_col": "id"},
    "/api/v1/nomcom/position/"                     : {"mirror": True,  "uri_col": "id"},
    "/api/v1/nomcom/reminderdates/"                : {"mirror": True,  "uri_col": "id"},
    "/api/v1/nomcom/topic/"                        : {"mirror": True,  "uri_col": "id"},
    "/api/v1/nomcom/topicfeedbacklastseen/"        : {"mirror": True,  "uri_col": "id"},
    "/api/v1/nomcom/volunteer/"                    : {"mirror": True,  "uri_col": "id"},
    "/api/v1/person/alias/"                        : {"mirror": True,  "uri_col": "id"},
    "/api/v1/person/email/"                        : {"mirror": True,  "uri_col": "address"},
    "/api/v1/person/historicalemail/"              : {"mirror": True,  "uri_col": "history_id"},
    "/api/v1/person/historicalperson/"             : {"mirror": True,  "uri_col": "history_id"},
    "/api/v1/person/person/"                       : {"mirror": True,  "uri_col": "id"},
    "/api/v1/person/personalapikey/"               : {"mirror": False, "uri_col": None}, # Unavailable in the public datatracker API
    "/api/v1/person/personapikeyevent/"            : {"mirror": False, "uri_col": None}, # Not useful: a record of datatracker login events
    "/api/v1/person/personevent/"                  : {"mirror": False, "uri_col": None}, # Not useful: a record of datatracker login events
    "/api/v1/person/personextresource/"            : {"mirror": True,  "uri_col": "id"},
    "/api/v1/redirects/command/"                   : {"mirror": False, "uri_col": None}, # Not useful
    "/api/v1/redirects/redirect/"                  : {"mirror": False, "uri_col": None}, # Not useful
    "/api/v1/redirects/suffix/"                    : {"mirror": False, "uri_col": None}, # Not useful
    "/api/v1/review/historicalreviewassignment/"   : {"mirror": True,  "uri_col": "history_id"},
    "/api/v1/review/historicalreviewersettings/"   : {"mirror": True,  "uri_col": "history_id"},
    "/api/v1/review/historicalreviewrequest/"      : {"mirror": True,  "uri_col": "history_id"},
    "/api/v1/review/historicalunavailableperiod/"  : {"mirror": False, "uri_col": None}, # This endpoint seems to throw errors
    "/api/v1/review/nextreviewerinteam/"           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/review/reviewassignment/"             : {"mirror": True,  "uri_col": "id"},
    "/api/v1/review/reviewersettings/"             : {"mirror": True,  "uri_col": "id"},
    "/api/v1/review/reviewrequest/"                : {"mirror": True,  "uri_col": "id"},
    "/api/v1/review/reviewsecretarysettings/"      : {"mirror": True,  "uri_col": "id"},
    "/api/v1/review/reviewteamsettings/"           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/review/reviewwish/"                   : {"mirror": True,  "uri_col": "id"},
    "/api/v1/review/unavailableperiod/"            : {"mirror": True,  "uri_col": "id"},
    "/api/v1/stats/affiliationalias/"              : {"mirror": True,  "uri_col": "id"},
    "/api/v1/stats/affiliationignoredending/"      : {"mirror": True,  "uri_col": "id"},
    "/api/v1/stats/countryalias/"                  : {"mirror": True,  "uri_col": "id"},
    "/api/v1/stats/meetingregistration/"           : {"mirror": True,  "uri_col": "id"},
    "/api/v1/submit/preapproval/"                  : {"mirror": True,  "uri_col": "id"},
    "/api/v1/submit/submission/"                   : {"mirror": True,  "uri_col": "id"},
    "/api/v1/submit/submissioncheck/"              : {"mirror": True,  "uri_col": "id"},
    "/api/v1/submit/submissionemailevent/"         : {"mirror": False, "uri_col": None}, # Unused in the datatracker
    "/api/v1/submit/submissionevent/"              : {"mirror": True,  "uri_col": "id"},
    "/api/v1/submit/submissionextresource/"        : {"mirror": True,  "uri_col": "id"},
    "/api/v1/utils/dumpinfo/"                      : {"mirror": False, "uri_col": None}, # Unused in the datatracker
    "/api/v1/utils/versioninfo/"                   : {"mirror": True,  "uri_col": "id"},
}

def sql_type_for(schemas, endpoint, column):
    schema_type  = schemas[endpoint]["columns"][column]["type"]
    if schema_type == "string" or schema_type == "datetime" or schema_type == "date" or schema_type == "timedelta":
        return "TEXT"
    elif schema_type == "integer" or schema_type == "boolen":
        return "INTEGER"
    else:
        print(f"Cannot derive sql type for {endpoint} {column}")
        sys.exit(1)


def create_db_table(db_cursor, schemas, endpoint):
    print(f"Create table {endpoint}")
    schema  = schemas[endpoint]
    columns = []
    foreign = []
    for column in schema["columns"].values():
        if column["name"] == "resource_uri":
            continue
        if column['type'] == "string" or column['type'] == "datetime" or column['type'] == "date" or column['type'] == "timedelta":
            column_sql = f"  \"{column['name']}\" TEXT"
        elif column['type'] == "integer" or column['type'] == "boolean": 
            column_sql = f"  \"{column['name']}\" INTEGER"
        elif column['type'] == "to_one": 
            foreign_table = schema["to_one"][column["name"]]["refers_to_table"]
            foreign_endpt = schema["to_one"][column["name"]]["refers_to_endpoint"]
            foreign_col   = endpoints_to_mirror[foreign_endpt]['uri_col']
            if foreign_endpt in schemas:
                    foreign.append(f"  FOREIGN KEY (\"{column['name']}\") REFERENCES {foreign_table} (\"{foreign_col}\")")
                    column_sql  = f"  \"{column['name']}\" {sql_type_for(schemas, foreign_endpt, foreign_col)}"
            else:
                    # The foreign endpoint is not one we mirror. Just store the content as text.
                    # e.g., /api/v1/nomcom/nomination/ refers to /api/v1/nomcom/feedback/'
                    column['type'] = "string"
                    column_sql = f"  \"{column['name']}\" TEXT"
        elif column['type'] == "to_many": 
            foreign_table  = schema["to_many"][column["name"]]["refers_to_table"]
            foreign_endpt  = schema["to_many"][column["name"]]["refers_to_endpoint"]
            column_current = schema['table'].split('_')[-1]
            column_foreign = column['name']
            sql  = f"CREATE TABLE {schema['table']}_{column['name']} (\n"
            sql += f"  \"id\" INTEGER PRIMARY KEY,\n"
            sql += f"  \"{column_current}\" {sql_type_for(schemas, endpoint, endpoints_to_mirror[endpoint]['uri_col'])},\n"
            sql += f"  \"{column_foreign}\" {sql_type_for(schemas, foreign_endpt, endpoints_to_mirror[foreign_endpt]['uri_col'])},\n"
            sql += f"  FOREIGN KEY (\"{column_current}\") REFERENCES {schema['table']} ({endpoints_to_mirror[endpoint]['uri_col']}),\n"
            sql += f"  FOREIGN KEY (\"{column_foreign}\") REFERENCES {foreign_table} ({endpoints_to_mirror[foreign_endpt]['uri_col']})\n"
            sql += f");\n"
            db_cursor.execute(sql)
            continue
        elif column['type'] == None:
            continue
        else:
            print(f"unknown column type {column['type']} (create_db_table)")
            sys.exit(1)

        if column["unique"]:
            column_sql += " UNIQUE"
        if column["name"] == endpoints_to_mirror[endpoint]['uri_col']:
            column_sql += " PRIMARY KEY"
        columns.append(column_sql)
    sql = f"CREATE TABLE {schema['table']} (\n"
    sql += ",\n".join(columns)
    if len(foreign) > 0:
        sql += ",\n"
        sql += ",\n".join(foreign)
    sql += "\n);\n"
    db_cursor.execute(sql)
    uri_col = endpoints_to_mirror[endpoint]['uri_col']
    sql = f"CREATE UNIQUE INDEX index_{schema['table']}_{uri_col} ON {schema['table']}(\"{uri_col}\")"
    db_cursor.execute(sql)



def import_db_table(db_cursor, db_connection, schemas, endpoint, dt):
    print(f"Import table {endpoint}")
    schema  = schemas[endpoint]

    vcount  = 0
    ordered = False
    for column in schema["columns"].values():
        if column['name'] == schema['sort_by']:
            ordered = True
        elif column['name'] == "resource_uri":
            continue
        if column['type'] in ["string", "date", "datetime", "timedelta", "integer", "boolean", "to_one"]: 
            vcount += 1
        elif column['type'] == "to_many": 
            continue
        elif column['type'] == None:
            continue
        else:
            print(f"unknown column type {column['type']} (import_db_table #1)")
            sys.exit(1)

    sql = f"INSERT INTO {schema['table']} VALUES(" + ",".join("?" * vcount) + ")"
    val = []
    if ordered:
        uri = f"{endpoint}?limit=500&order_by={schema['sort_by']}"
    else:
        uri = f"{endpoint}?limit=500"

    for item in dt.fetch_multi(uri):
        #print(f"  {item['resource_uri']}")
        values = []
        for column in schema["columns"].values():
            if column["name"] == "resource_uri":
                continue
            if column['type'] in ["string", "integer", "boolean", "date", "timedelta"]:
                values.append(item[column["name"]])
            elif column['type'] == "datetime": 
                if item[column["name"]] is None:
                    values.append(None)
                else:
                    # FIXME: check this correctly converts to UTC
                    dt_val = datetime.datetime.fromisoformat(item[column["name"]])
                    dt_fmt = dt_val.astimezone(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
                    values.append(dt_fmt)
            elif column['type'] == "to_one": 
                    if item[column["name"]] is None:
                        values.append(None)
                    else:
                        values.append(item[column["name"]].split("/")[-2])
            elif column['type'] == "to_many": 
                column_current = schema['table'].split('_')[-1]
                column_foreign = column['name']
                subtable_sql = f"INSERT INTO {schema['table']}_{column['name']} (\"{column_current}\", \"{column_foreign}\") VALUES(?, ?)"
                subtable_val = []
                for subtable_item in item[column['name']]:
                    subtable_val.append((item[endpoints_to_mirror[endpoint]['uri_col']], subtable_item.split("/")[-2]))
                db_cursor.executemany(subtable_sql, subtable_val)
                continue
            elif column['type'] == None:
                continue
            else:
                print(f"unknown column type {column['type']} (import_db_table #2)")
                sys.exit(1)
        val.append(tuple(values))
    db_cursor.executemany(sql, val)
    db_connection.commit()


# =================================================================================================
# Main code follows:

if len(sys.argv) == 2:
    database_file = sys.argv[1]
else:
    print("Usage: scripts/db-from-ietf-datatracker.py <database.db>")
    sys.exit(1)

url = os.environ.get("IETFDATA_DT_URL", "https://datatracker.ietf.org/")
dt  = Datatracker(url)

print(f"db-from-ietf-datatracker.py: {database_file} {url}")

db_connection = sqlite3.connect(database_file)
db_connection.execute('PRAGMA synchronous = 0;') # Don't force fsync on the file between writes

db_cursor = db_connection.cursor()

# Find the endpoints to mirror and fetch their database schema:
schemas   = {}
endpoints = []
print("Extracting API endpoints:") 
for endpoint in dt.api_endpoints():
    print(f"  {endpoint}")
    if endpoint not in endpoints_to_mirror:
        print(f"ERROR: mirroring for {endpoint} not configured")
    else:
        if endpoints_to_mirror[endpoint]["mirror"]:
            endpoints.append(endpoint)
            schemas[endpoint] = dt.schema_for_endpoint(endpoint)
print("")

# Find the to_one and to_many mappings:
print("Extracting to_one and to_many mappings:")
for endpoint in endpoints:
    print(f"  {endpoint}")
    schema = schemas[endpoint]
    schema["to_one"]  = {}
    schema["to_many"] = {}

    ordered = False
    for column in schema["columns"].values():
        if column['name'] == schema['sort_by']:
            ordered = True
    if ordered:
        uri = f"{endpoint}?limit=500&order_by={schema['sort_by']}"
    else:
        uri = f"{endpoint}?limit=500"

    for item in dt.fetch_multi(uri):
        found_all = True
        for column in schema["columns"].values():
            if column["type"] == "to_one":
                if column["name"] not in schema["to_one"]:
                    if item[column["name"]] is not None and item[column["name"]] != "":
                        to_one = {
                            "refers_to_endpoint": "/".join(item[column["name"]].split("/")[:-2]) + "/",
                            "refers_to_table": "ietf_dt_" + "_".join(item[column["name"]].split("/")[3:-2])
                        }
                        schema["to_one"][column["name"]] = to_one
                        print(f"    {column['name']} -> {to_one['refers_to_table']}")
                if column["name"] not in schema["to_one"]:
                    found_all = False
            if column["type"] == "to_many":
                if column["name"] not in schema["to_many"]:
                    val = item[column["name"]]
                    if item[column["name"]] is not None and item[column["name"]] != "" and len(item[column["name"]]) > 0:
                        to_many = {
                            "refers_to_endpoint": "/".join(item[column["name"]][0].split("/")[:-2]) + "/",
                            "refers_to_table": "ietf_dt_" + "_".join(item[column["name"]][0].split("/")[3:-2])
                        }
                        schema["to_many"][column["name"]] = to_many
                        print(f"    {column['name']} -> {to_many['refers_to_table']} (many)")
                if column["name"] not in schema["to_many"]:
                    found_all = False
        if found_all:
            break
    for column in schema["columns"].values():
        if column["type"] == "to_one" and not column["name"] in schema["to_one"]:
            print(f"    {column['name']} is to_one but not used")
            column["type"] = None
        if column["type"] == "to_many" and not column["name"] in schema["to_many"]:
            print(f"    {column['name']} is to_many but not used")
            column["type"] = None
print("")

# Create the database tables:
for endpoint in endpoints:
    create_db_table(db_cursor, schemas, endpoint)
print("")

# Populate the database tables:
for endpoint in endpoints:
    import_db_table(db_cursor, db_connection, schemas, endpoint, dt)

print("Vacuuming database")
db_connection.execute('VACUUM;') # Don't force fsync on the file between writes

# vim: set tw=0 ai et:
