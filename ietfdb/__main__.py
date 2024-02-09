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

import requests
import sqlite3
import sys
import json

from datetime import datetime
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
        if Path("cache.json").exists():
            with open("cache.json", "r") as inf:
                self.cache = json.load(inf)
                print(f"Loaded {len(self.cache)} items from cache")


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
                r2 = self.session.get(f"https://datatracker.ietf.org{category['list_endpoint']}")
                if r2.status_code == 200:
                    for endpoint in r2.json().values():
                        yield endpoint["list_endpoint"]
                else:
                    print(f"Cannot fetch secondary API endpoints: {r2.status_code}")
                    sys.exit(1)
        else:
            print(f"Cannot fetch top-level API endpoints: {r1.status_code}")
            sys.exit(1)


    def schema_for_endpoint(self, api_endpoint:str):
        resp = self.session.get(f"https://datatracker.ietf.org{endpoint}schema")
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
            print(f"ERROR: {resp.status_code}")
            sys.exit(1)


endpoints_to_mirror = {
        "/api/v1/community/communitylist/"             : {"mirror": False},
        "/api/v1/community/emailsubscription/"         : {"mirror": False},
        "/api/v1/community/searchrule/"                : {"mirror": False},
        "/api/v1/dbtemplate/dbtemplate/"               : {"mirror": False},
        "/api/v1/doc/addedmessageevent/"               : {"mirror": False},
        "/api/v1/doc/ballotdocevent/"                  : {"mirror": False},
		"/api/v1/doc/ballotpositiondocevent/"          : {"mirror": False},
		"/api/v1/doc/ballottype/"                      : {"mirror": False},
		"/api/v1/doc/bofreqeditordocevent/"            : {"mirror": False},
		"/api/v1/doc/bofreqresponsibledocevent/"       : {"mirror": False},
		"/api/v1/doc/consensusdocevent/"               : {"mirror": False},
		"/api/v1/doc/deletedevent/"                    : {"mirror": False},
		"/api/v1/doc/docevent/"                        : {"mirror": False},
		"/api/v1/doc/docextresource/"                  : {"mirror": False},
		"/api/v1/doc/dochistory/"                      : {"mirror": False},
		"/api/v1/doc/dochistoryauthor/"                : {"mirror": False},
		"/api/v1/doc/docreminder/"                     : {"mirror": False},
		"/api/v1/doc/document/"                        : {"mirror": True},
		"/api/v1/doc/documentactionholder/"            : {"mirror": False},
		"/api/v1/doc/documentauthor/"                  : {"mirror": False},
		"/api/v1/doc/documenturl/"                     : {"mirror": False},
		"/api/v1/doc/editedauthorsdocevent/"           : {"mirror": False},
		"/api/v1/doc/ianaexpertdocevent/"              : {"mirror": False},
		"/api/v1/doc/initialreviewdocevent/"           : {"mirror": False},
		"/api/v1/doc/irsgballotdocevent/"              : {"mirror": False},
		"/api/v1/doc/lastcalldocevent/"                : {"mirror": False},
		"/api/v1/doc/newrevisiondocevent/"             : {"mirror": False},
		"/api/v1/doc/relateddochistory/"               : {"mirror": False},
		"/api/v1/doc/relateddocument/"                 : {"mirror": False},
		"/api/v1/doc/reviewassignmentdocevent/"        : {"mirror": False},
		"/api/v1/doc/reviewrequestdocevent/"           : {"mirror": False},
		"/api/v1/doc/state/"                           : {"mirror": True},
		"/api/v1/doc/statedocevent/"                   : {"mirror": False},
		"/api/v1/doc/statetype/"                       : {"mirror": True},
		"/api/v1/doc/submissiondocevent/"              : {"mirror": False},
		"/api/v1/doc/telechatdocevent/"                : {"mirror": False},
		"/api/v1/doc/writeupdocevent/"                 : {"mirror": False},
		"/api/v1/group/appeal/"                        : {"mirror": False},
		"/api/v1/group/appealartifact/"                : {"mirror": False},
		"/api/v1/group/changestategroupevent/"         : {"mirror": False},
		"/api/v1/group/group/"                         : {"mirror": True},
		"/api/v1/group/groupevent/"                    : {"mirror": False},
		"/api/v1/group/groupextresource/"              : {"mirror": False},
		"/api/v1/group/groupfeatures/"                 : {"mirror": False},
		"/api/v1/group/grouphistory/"                  : {"mirror": False},
		"/api/v1/group/groupmilestone/"                : {"mirror": False},
		"/api/v1/group/groupmilestonehistory/"         : {"mirror": False},
		"/api/v1/group/groupstatetransitions/"         : {"mirror": False},
		"/api/v1/group/groupurl/"                      : {"mirror": False},
		"/api/v1/group/milestonegroupevent/"           : {"mirror": False},
		"/api/v1/group/role/"                          : {"mirror": False},
		"/api/v1/group/rolehistory/"                   : {"mirror": False},
		"/api/v1/iesg/telechat/"                       : {"mirror": False},
		"/api/v1/iesg/telechatagendacontent/"          : {"mirror": False},
		"/api/v1/iesg/telechatagendaitem/"             : {"mirror": False},
		"/api/v1/iesg/telechatdate/"                   : {"mirror": False},
		"/api/v1/ipr/genericiprdisclosure/"            : {"mirror": False},
		"/api/v1/ipr/holderiprdisclosure/"             : {"mirror": False},
		"/api/v1/ipr/iprdisclosurebase/"               : {"mirror": False},
		"/api/v1/ipr/iprdocrel/"                       : {"mirror": False},
		"/api/v1/ipr/iprevent/"                        : {"mirror": False},
		"/api/v1/ipr/legacymigrationiprevent/"         : {"mirror": False},
		"/api/v1/ipr/nondocspecificiprdisclosure/"     : {"mirror": False},
		"/api/v1/ipr/relatedipr/"                      : {"mirror": False},
		"/api/v1/ipr/thirdpartyiprdisclosure/"         : {"mirror": False},
		"/api/v1/liaisons/liaisonstatement/"           : {"mirror": False},
		"/api/v1/liaisons/liaisonstatementattachment/" : {"mirror": False},
		"/api/v1/liaisons/liaisonstatementevent/"      : {"mirror": False},
		"/api/v1/liaisons/relatedliaisonstatement/"    : {"mirror": False},
		"/api/v1/mailinglists/allowlisted/"            : {"mirror": False},
		"/api/v1/mailinglists/list/"                   : {"mirror": False},
		"/api/v1/mailinglists/subscribed/"             : {"mirror": False},
		"/api/v1/mailtrigger/mailtrigger/"             : {"mirror": False},
		"/api/v1/mailtrigger/recipient/"               : {"mirror": False},
		"/api/v1/meeting/attended/"                    : {"mirror": False},
		"/api/v1/meeting/businessconstraint/"          : {"mirror": False},
		"/api/v1/meeting/constraint/"                  : {"mirror": False},
		"/api/v1/meeting/floorplan/"                   : {"mirror": False},
		"/api/v1/meeting/importantdate/"               : {"mirror": False},
		"/api/v1/meeting/meeting/"                     : {"mirror": False},
		"/api/v1/meeting/meetinghost/"                 : {"mirror": False},
		"/api/v1/meeting/proceedingsmaterial/"         : {"mirror": False},
		"/api/v1/meeting/resourceassociation/"         : {"mirror": False},
		"/api/v1/meeting/room/"                        : {"mirror": False},
		"/api/v1/meeting/schedtimesessassignment/"     : {"mirror": False},
		"/api/v1/meeting/schedule/"                    : {"mirror": False},
		"/api/v1/meeting/schedulingevent/"             : {"mirror": False},
		"/api/v1/meeting/session/"                     : {"mirror": False},
		"/api/v1/meeting/sessionpresentation/"         : {"mirror": False},
		"/api/v1/meeting/slidesubmission/"             : {"mirror": False},
		"/api/v1/meeting/timeslot/"                    : {"mirror": False},
		"/api/v1/meeting/urlresource/"                 : {"mirror": False},
		"/api/v1/message/announcementfrom/"            : {"mirror": False},
		"/api/v1/message/message/"                     : {"mirror": False},
		"/api/v1/message/messageattachment/"           : {"mirror": False},
		"/api/v1/message/sendqueue/"                   : {"mirror": False},
		"/api/v1/name/agendafiltertypename/"           : {"mirror": False},
		"/api/v1/name/agendatypename/"                 : {"mirror": False},
		"/api/v1/name/appealartifacttypename/"         : {"mirror": False},
		"/api/v1/name/ballotpositionname/"             : {"mirror": False},
		"/api/v1/name/constraintname/"                 : {"mirror": False},
		"/api/v1/name/continentname/"                  : {"mirror": False},
		"/api/v1/name/countryname/"                    : {"mirror": False},
		"/api/v1/name/dbtemplatetypename/"             : {"mirror": False},
		"/api/v1/name/docrelationshipname/"            : {"mirror": False},
		"/api/v1/name/docremindertypename/"            : {"mirror": False},
		"/api/v1/name/doctagname/"                     : {"mirror": False},
		"/api/v1/name/doctypename/"                    : {"mirror": True},
		"/api/v1/name/docurltagname/"                  : {"mirror": False},
		"/api/v1/name/draftsubmissionstatename/"       : {"mirror": False},
		"/api/v1/name/extresourcename/"                : {"mirror": False},
		"/api/v1/name/extresourcetypename/"            : {"mirror": False},
		"/api/v1/name/feedbacktypename/"               : {"mirror": False},
		"/api/v1/name/formallanguagename/"             : {"mirror": False},
		"/api/v1/name/groupmilestonestatename/"        : {"mirror": False},
		"/api/v1/name/groupstatename/"                 : {"mirror": True},
		"/api/v1/name/grouptypename/"                  : {"mirror": True},
		"/api/v1/name/importantdatename/"              : {"mirror": False},
		"/api/v1/name/intendedstdlevelname/"           : {"mirror": True},
		"/api/v1/name/iprdisclosurestatename/"         : {"mirror": False},
		"/api/v1/name/ipreventtypename/"               : {"mirror": False},
		"/api/v1/name/iprlicensetypename/"             : {"mirror": False},
		"/api/v1/name/liaisonstatementeventtypename/"  : {"mirror": False},
		"/api/v1/name/liaisonstatementpurposename/"    : {"mirror": False},
		"/api/v1/name/liaisonstatementstate/"          : {"mirror": False},
		"/api/v1/name/liaisonstatementtagname/"        : {"mirror": False},
		"/api/v1/name/meetingtypename/"                : {"mirror": False},
		"/api/v1/name/nomineepositionstatename/"       : {"mirror": False},
		"/api/v1/name/proceedingsmaterialtypename/"    : {"mirror": False},
		"/api/v1/name/reviewassignmentstatename/"      : {"mirror": False},
		"/api/v1/name/reviewerqueuepolicyname/"        : {"mirror": False},
		"/api/v1/name/reviewrequeststatename/"         : {"mirror": False},
		"/api/v1/name/reviewresultname/"               : {"mirror": False},
		"/api/v1/name/reviewtypename/"                 : {"mirror": False},
		"/api/v1/name/rolename/"                       : {"mirror": False},
		"/api/v1/name/roomresourcename/"               : {"mirror": False},
		"/api/v1/name/sessionpurposename/"             : {"mirror": False},
		"/api/v1/name/sessionstatusname/"              : {"mirror": False},
		"/api/v1/name/slidesubmissionstatusname/"      : {"mirror": False},
		"/api/v1/name/stdlevelname/"                   : {"mirror": True},
		"/api/v1/name/streamname/"                     : {"mirror": True},
		"/api/v1/name/telechatagendasectionname/"      : {"mirror": False},
		"/api/v1/name/timerangename/"                  : {"mirror": False},
		"/api/v1/name/timeslottypename/"               : {"mirror": False},
		"/api/v1/name/topicaudiencename/"              : {"mirror": False},
		"/api/v1/nomcom/feedback/"                     : {"mirror": False},
		"/api/v1/nomcom/feedbacklastseen/"             : {"mirror": False},
		"/api/v1/nomcom/nomcom/"                       : {"mirror": False},
		"/api/v1/nomcom/nomination/"                   : {"mirror": False},
		"/api/v1/nomcom/nominee/"                      : {"mirror": False},
		"/api/v1/nomcom/nomineeposition/"              : {"mirror": False},
		"/api/v1/nomcom/position/"                     : {"mirror": False},
		"/api/v1/nomcom/reminderdates/"                : {"mirror": False},
		"/api/v1/nomcom/topic/"                        : {"mirror": False},
		"/api/v1/nomcom/topicfeedbacklastseen/"        : {"mirror": False},
		"/api/v1/nomcom/volunteer/"                    : {"mirror": False},
		"/api/v1/person/alias/"                        : {"mirror": False},
		"/api/v1/person/email/"                        : {"mirror": True},
		"/api/v1/person/historicalemail/"              : {"mirror": False},
		"/api/v1/person/historicalperson/"             : {"mirror": False},
		"/api/v1/person/person/"                       : {"mirror": True},
		"/api/v1/person/personalapikey/"               : {"mirror": False},
		"/api/v1/person/personapikeyevent/"            : {"mirror": False},
		"/api/v1/person/personevent/"                  : {"mirror": False},
		"/api/v1/person/personextresource/"            : {"mirror": False},
		"/api/v1/redirects/command/"                   : {"mirror": False},
		"/api/v1/redirects/redirect/"                  : {"mirror": False},
		"/api/v1/redirects/suffix/"                    : {"mirror": False},
		"/api/v1/review/historicalreviewassignment/"   : {"mirror": False},
		"/api/v1/review/historicalreviewersettings/"   : {"mirror": False},
		"/api/v1/review/historicalreviewrequest/"      : {"mirror": False},
		"/api/v1/review/historicalunavailableperiod/"  : {"mirror": False},
		"/api/v1/review/nextreviewerinteam/"           : {"mirror": False},
		"/api/v1/review/reviewassignment/"             : {"mirror": False},
		"/api/v1/review/reviewersettings/"             : {"mirror": False},
		"/api/v1/review/reviewrequest/"                : {"mirror": False},
		"/api/v1/review/reviewsecretarysettings/"      : {"mirror": False},
		"/api/v1/review/reviewteamsettings/"           : {"mirror": False},
		"/api/v1/review/reviewwish/"                   : {"mirror": False},
		"/api/v1/review/unavailableperiod/"            : {"mirror": False},
		"/api/v1/stats/affiliationalias/"              : {"mirror": False},
		"/api/v1/stats/affiliationignoredending/"      : {"mirror": False},
		"/api/v1/stats/countryalias/"                  : {"mirror": False},
		"/api/v1/stats/meetingregistration/"           : {"mirror": False},
		"/api/v1/submit/preapproval/"                  : {"mirror": False},
		"/api/v1/submit/submission/"                   : {"mirror": False},
		"/api/v1/submit/submissioncheck/"              : {"mirror": False},
		"/api/v1/submit/submissionemailevent/"         : {"mirror": False},
		"/api/v1/submit/submissionevent/"              : {"mirror": False},
		"/api/v1/submit/submissionextresource/"        : {"mirror": False},
		"/api/v1/utils/dumpinfo/"                      : {"mirror": False},
		"/api/v1/utils/versioninfo/"                   : {"mirror": False},
}


def create_db_table(db_cursor, schemas, endpoint):
    schema  = schemas[endpoint]
    columns = []
    foreign = []
    for column in schema["columns"].values():
        if column['type'] == "string" or column['type'] == "datetime":
            column_sql = f"  \"{column['name']}\" TEXT"
        elif column['type'] == "integer" or column['type'] == "boolean": 
            column_sql = f"  \"{column['name']}\" INTEGER"
        elif column['type'] == "to_one": 
            foreign_api   = schema["to_one"][column["name"]]["refers_to_endpoint"]
            foreign_table = schema["to_one"][column["name"]]["refers_to_table"]
            foreign_type  = schemas[foreign_api]["columns"][schemas[foreign_api]["primary_key"]]["type"]
            foreign_key   = schemas[foreign_api]["primary_key"]
            if foreign_type == "string" or foreign_type == "datetime":
                column_sql = f"  \"{column['name']}\" TEXT"
            elif foreign_type == "integer":
                column_sql = f"  \"{column['name']}\" INTEGER"
            else:
                print(f"unknown foreign type {column['type']}")
                sys.exit(1)
            foreign.append(f"  FOREIGN KEY (\"{column['name']}\") REFERENCES {foreign_table} (\"{foreign_key}\")")
        elif column['type'] == "to_many": 
            # FIXME
            #column_sql = f"  \"{column['name']}\" TEXT"
            continue
        elif column['type'] == None:
            continue
        else:
            print(f"unknown column type {column['type']}")
            sys.exit(1)

        if column["primary"]:
            column_sql += " PRIMARY KEY"
        columns.append(column_sql)
    sql = f"CREATE TABLE {schema['table']} (\n"
    sql += ",\n".join(columns)
    if len(foreign) > 0:
        sql += ",\n"
        sql += ",\n".join(foreign)
    sql += "\n);\n"
    print(sql)
    db_cursor.execute(sql)



def import_db_table(db_cursor, db_connection, schemas, endpoint, dt):
    schema  = schemas[endpoint]

    vcount  = 0
    ordered = False
    for column in schema["columns"].values():
        if column['name'] == schema['sort_by']:
            ordered = True
        if column['type'] in ["string", "datetime", "integer", "boolean", "to_one"]: 
            vcount += 1
        elif column['type'] == "to_many": 
            continue
        elif column['type'] == None:
            continue
        else:
            print(f"unknown column type {column['type']}")
            sys.exit(1)

    sql = f"INSERT INTO {schema['table']} VALUES(" + ",".join("?" * vcount) + ")"
    if ordered:
        uri = f"{endpoint}?order_by={schema['sort_by']}"
    else:
        uri = endpoint

    for item in dt.fetch_multi(uri):
        print(item["resource_uri"])
        values = []
        for column in schema["columns"].values():
            if column['type'] in ["string", "datetime", "integer", "boolean"]:
                values.append(item[column["name"]])
            elif column['type'] == "to_one": 
                foreign_api   = schema["to_one"][column["name"]]["refers_to_endpoint"]
                # FIXME: some foreign keys don't refer to the primary key of the respective table 
                # (e.g., /api/v1/doc/document/ references are to "name" but the primary key is "id").
                # We may need to search through all the "unique" fields to find the one to match.
                foreign_type  = schemas[foreign_api]["columns"][schemas[foreign_api]["primary_key"]]["type"]
                if item[column["name"]] is None:
                    values.append(None)
                else:
                    foreign_value = item[column["name"]].split("/")[-2]
                    if foreign_type == "string" or foreign_type == "datetime":
                        values.append(foreign_value)
                    elif foreign_type == "integer":
                        values.append(int(foreign_value))
                    else:
                        print(f"unknown foreign type {column['type']}")
                        sys.exit(1)
            elif column['type'] == "to_many": 
                # FIXME
                continue
            elif column['type'] == None:
                continue
            else:
                print(f"unknown column type {column['type']}")
                sys.exit(1)
        db_cursor.execute(sql, tuple(values))
        db_connection.commit()



#dt = Datatracker("http://dundas:8000/")
dt = Datatracker("http://localhost:8000/")
db_connection = sqlite3.connect("ietf.db")
db_cursor     = db_connection.cursor()

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
        uri = f"{endpoint}?order_by={schema['sort_by']}"
    else:
        uri = endpoint

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
                            "refers_to": "ietf_dt_" + "_".join(item[column["name"]][0].split("/")[3:-2])
                        }
                        schema["to_many"][column["name"]] = to_many
                        print(f"    {column['name']} -> {to_many['refers_to']} (many)")
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

with open("cache.json", "w") as outf:
    json.dump(dt.cache, outf, indent=3)

# Create the database tables:
for endpoint in endpoints:
    create_db_table(db_cursor, schemas, endpoint)

# Populate the database tables:
for endpoint in endpoints:
    import_db_table(db_cursor, db_connection, schemas, endpoint, dt)
    with open("cache.json", "w") as outf:
        json.dump(dt.cache, outf, indent=3)

