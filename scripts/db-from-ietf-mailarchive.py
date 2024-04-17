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
import sys
import sqlite3
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from email              import policy, utils
from email.parser       import BytesParser
from email.message      import Message
from email.utils        import parseaddr, parsedate_to_datetime, getaddresses
from imapclient         import IMAPClient
from pathlib            import Path

# =================================================================================================
# Helper functions

def fetch_folder(folder_name, archive_dir):
    # Create a new connection to the IMAP server for this thread:
    imap = IMAPClient(host='imap.ietf.org', ssl=True, use_uid=True)
    imap.login("anonymous", "anonymous")

    _, _, imap_ns_shared = imap.namespace()
    imap_prefix    = imap_ns_shared[0][0]
    imap_separator = imap_ns_shared[0][1]

    folder_info = imap.select_folder(folder_name, readonly=True)

    modified = False
    # Fetch new messages:
    for msg_id in imap.search(['NOT', 'DELETED']):
        folder_path = Path(f"{archive_dir}/{folder_name[len(imap_prefix):]}")
        msg_path    = Path(folder_path / f"{msg_id}.eml")  # FIXME: zero-prefixed?
        if not msg_path.exists():
            msg = imap.fetch(msg_id, ["RFC822"])
            if msg == {}:
                print(f"      {msg_path} is unavailable")
            else:
                tmp_path = msg_path.with_suffix(".tmp")
                assert b'RFC822' in msg[msg_id]
                with open(tmp_path, "wb") as outf:
                    outf.write(msg[msg_id][b"RFC822"])
                tmp_path.replace(msg_path)
                print(f"      {msg_path}")
                modified = True

    # Save metadata:
    folder_path = Path(f"{archive_dir}/{folder_name[len(imap_prefix):]}")
    folder_path.mkdir(parents=True, exist_ok=True)
    meta_path = folder_path / "meta.json"
    with open(meta_path, "w") as outf:
        folder = {}
        folder["name"]        = folder_name
        folder["uidvalidity"] = folder_info[b'UIDVALIDITY']
        folder["uidnext"]     = folder_info[b'UIDNEXT']
        json.dump(folder, outf, indent=2)

    return modified


def download_all(archive_dir):
    print("    Downloading messages:")
    with ThreadPoolExecutor(max_workers=16) as executor:
        # Login to the IETF mail archive using IMAP:
        imap = IMAPClient(host='imap.ietf.org', ssl=True, use_uid=True)
        imap.login("anonymous", "anonymous")

        _, _, imap_ns_shared = imap.namespace()
        imap_prefix    = imap_ns_shared[0][0]
        imap_separator = imap_ns_shared[0][1]
        folder_list    = imap.list_folders()

        tasks = {}
        for flags, delimiter, name in folder_list:
            if b'\\Noselect' in flags:
                continue

            # Load current folder metadata:
            folder_info = imap.select_folder(name, readonly=True)
            folder = {}
            folder["name"]        = name
            folder["uidvalidity"] = folder_info[b'UIDVALIDITY']
            folder["uidnext"]     = folder_info[b'UIDNEXT']

            # Load previous folder metadata:
            folder_path = Path(f"{archive_dir}/{name[len(imap_prefix):]}")
            folder_path.mkdir(parents=True, exist_ok=True)

            print(f"      {folder_path}")

            meta_path = folder_path / "meta.json"
            if meta_path.exists():
                with open(meta_path, "r") as inf:
                    prev_state = json.load(inf)
            else:
                prev_state = {}
                prev_state["name"]        = name
                prev_state["uidvalidity"] = None
                prev_state["uidnext"]     = None

            # Do we need to update this folder?
            clean = False
            fetch = False
            if folder["uidvalidity"] != prev_state["uidvalidity"]:
                clean = True
                fetch = True
            if folder["uidnext"] != prev_state["uidnext"]:
                fetch = True

            if clean:
                print(f"WARNING: UIDVALIDITY changed {name}")
                for msg_path in sorted(folder_path.glob("*.eml")):
                    print(f"      {msg_path} removed")
                    msg_path.unlink()
                if meta_path.exists():
                    print(f"      {meta_path} removed")
                    meta_path.unlink()

            if fetch:
                future = executor.submit(fetch_folder, name, archive_dir)
                tasks[future] = name

        # We need to join with the threads as they complete and evaluate
        # the result of the future to ensure exceptions are propagated.
        modified = False
        for future in as_completed(tasks):
            modified |= future.result()

        return folder_list


# =================================================================================================

def fixaddr(old_addr) -> str:
    addr = old_addr

    if addr is None:
        return None

    # Rewrite arnaud.taddei=40broadcom.com@dmarc.ietf.org to arnaud.taddei@broadcom.com
    if addr.endswith("@dmarc.ietf.org"):
        addr = addr[:-15].replace("=40", "@")

    # Rewrite "Michelle Claudé <Michelle.Claude@prism.uvsq.fr>"@prism.uvsq.fr to Michelle.Claude@prism.uvsq.fr
    # or "minshall@wc.novell.com"@decpa.enet.dec.com to minshall@wc.novell.com
    if addr.count("@") == 2:
        lpart = addr.split("@")[0]
        cpart = addr.split("@")[1]
        rpart = addr.split("@")[2]
        if lpart.startswith('"') and cpart.endswith('"'):
            lcomb = f"{lpart}@{cpart}"
            if lcomb.startswith("'") and lcomb.endswith("'"):
                lcomb = addr[1:-1]
            if lcomb.startswith('"') and lcomb.endswith('"'):
                lcomb = addr[1:-1]
            lname, laddr = parseaddr(lcomb)
            if laddr != '':
                addr = laddr

    # Rewrite lear at cisco.com to lear@cisco.com
    if " at " in addr:
        addr = addr.replace(" at ", "@")

    # Strip leading and trailing '
    if addr.startswith("'") and addr.endswith("'"):
        addr = addr[1:-1]

    # Strip leading and trailing "
    if addr.startswith('"') and addr.endswith('"'):
        addr = addr[1:-1]

    if addr != old_addr:
        print(f"          {old_addr} -> {addr}")
    return addr.strip()

# =================================================================================================
# Main code follows:

usage = "Usage: scripts/db-from-ietf-mailarchive.py [--embed] <database.db> <mailarchive_dir>"

if len(sys.argv) == 3:
    database_file = sys.argv[1]
    archive_dir   = sys.argv[2]
    embed         = False
elif len(sys.argv) == 4:
    if sys.argv[1] == "--embed":
        database_file = sys.argv[2]
        archive_dir   = sys.argv[3]
        embed         = True
        print("    Embedding message content in database")
    else:
        print(usage)
        sys.exit(1)
else:
    print(usage)
    sys.exit(1)

print(f"db-from-ietf-mailarchive.py: {database_file} {archive_dir}", end="")
if embed:
    print("    Embedding message contents in database")
else:
    print("")

folder_list = download_all(archive_dir)

db_connection = sqlite3.connect(database_file)
db_connection.execute('PRAGMA synchronous = 0;') # Don't force fsync on the file between writes
db_cursor = db_connection.cursor()

db_tables = list(map(lambda x : x[0], db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")))
has_dt_tables = True
for name in ["ietf_dt_person_email"]:
    if name not in db_tables:
        has_dt_tables = False

if has_dt_tables:
    print("  Database has ietf_dt_* tables")

sql =  f"CREATE TABLE ietf_ma_messages (\n"
sql += f"  message_num    INTEGER PRIMARY KEY,\n"
sql += f"  mailing_list   TEXT NOT NULL,\n"
sql += f"  uidvalidity    INTEGER NOT NULL,\n"
sql += f"  uid            INTEGER NOT NULL,\n"
sql += f"  from_name      TEXT,\n"
sql += f"  from_addr      TEXT,\n"
sql += f"  subject        TEXT,\n"
sql += f"  date           TEXT,\n"
sql += f"  date_unparsed  TEXT,\n"
sql += f"  message_id     TEXT,\n"
sql += f"  in_reply_to    TEXT,\n"
sql += f"  message        BLOB,\n"
if has_dt_tables:
    sql += f"  FOREIGN KEY (from_addr) REFERENCES ietf_dt_person_email (address),\n"
sql += f"  FOREIGN KEY (mailing_list) REFERENCES ietf_ma_lists (name)\n"
sql += ");\n"
db_cursor.execute(sql)

sql = f"CREATE INDEX index_ietf_ma_messages_from_addr   ON ietf_ma_messages(from_addr);\n"
db_cursor.execute(sql)
sql = f"CREATE INDEX index_ietf_ma_messages_message_id  ON ietf_ma_messages(message_id);\n"
db_cursor.execute(sql)
sql = f"CREATE INDEX index_ietf_ma_messages_in_reply_to ON ietf_ma_messages(in_reply_to);\n"
db_cursor.execute(sql)


sql =  f"CREATE TABLE ietf_ma_messages_to (\n"
sql += f"  id          INTEGER PRIMARY KEY,\n"
sql += f"  message_num INTEGER,\n"
sql += f"  to_name     TEXT,\n"
sql += f"  to_addr     TEXT,\n"
if has_dt_tables:
    sql += f"  FOREIGN KEY (to_addr) REFERENCES ietf_dt_person_email (address),\n"
sql += f"  FOREIGN KEY (message_num) REFERENCES ietf_ma_messages (message_num)\n"
sql += ");\n"
db_cursor.execute(sql)


sql =  f"CREATE TABLE ietf_ma_messages_cc (\n"
sql += f"  id          INTEGER PRIMARY KEY,\n"
sql += f"  message_num INTEGER,\n"
sql += f"  cc_name     TEXT,\n"
sql += f"  cc_addr     TEXT,\n"
if has_dt_tables:
    sql += f"  FOREIGN KEY (cc_addr) REFERENCES ietf_dt_person_email (address),\n"
sql += f"  FOREIGN KEY (message_num) REFERENCES ietf_ma_messages (message_num)\n"
sql += ");\n"
db_cursor.execute(sql)


err_count = 0
tot_count = 0

print("  Populating database:")
for imap_flags, imap_delimiter, imap_folder in folder_list:
    if b'\\Noselect' in imap_flags:
        continue
    folder_name = imap_folder.split(imap_delimiter.decode("utf-8"))[-1]
    folder_path = f"{archive_dir}/{folder_name}"

    print(f"     {folder_name}")

    with open(f"{folder_path}/meta.json", "r") as inf:
        meta = json.load(inf)

    for msg_path in sorted(Path(folder_path).glob("*.eml")):
        tot_count += 1

        if embed:
            with open(msg_path, "rb") as inf:
                message = inf.read()
        else:
            message = None
        with open(msg_path, "rb") as inf:
            msg = BytesParser(policy=policy.default).parse(inf)
            uidvalidity     = int(meta["uidvalidity"])
            uid             = int(msg_path.stem)
            hdr_from_name   = None
            hdr_from_addr   = None
            hdr_subject     = None
            hdr_date        = None
            hdr_message_id  = None
            hdr_in_reply_to = None
            parsed_date     = None
            try:
                hdr_from        = msg["from"]
                hdr_from_name, hdr_from_addr = parseaddr(hdr_from)
                hdr_subject     = msg["subject"]
                hdr_date        = msg["date"]
                hdr_message_id  = msg["message-id"]
                in_reply_to = msg["in-reply-to"]
                references  = msg["references"]
                if in_reply_to != "":
                    hdr_in_reply_to = in_reply_to
                elif references != "":
                    hdr_in_reply_to = references.strip().split(" ")[-1]
                parsed_date = parsedate_to_datetime(hdr_date).astimezone(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
            except:
                print(f"ERROR: cannot parse headers for {msg_path}")
                err_count += 1
            val = (tot_count,
                   folder_name,
                   uidvalidity,
                   uid,
                   hdr_from_name,
                   fixaddr(hdr_from_addr),
                   hdr_subject,
                   parsed_date,
                   hdr_date,
                   hdr_message_id,
                   hdr_in_reply_to,
                   message)
            sql = f"INSERT INTO ietf_ma_messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            db_cursor.execute(sql, val)

            # Insert "From:" addresses into the ietf_dt_person_email table.
            # These addresses are largely well-formed in the email archive,
            # unlike the "To:" or "Cc:" addresses.
            if has_dt_tables and hdr_from_addr is not None:
                val = (0, fixaddr(hdr_from_addr), f"mailarchive", None, 0, parsed_date);
                sql = f"INSERT or IGNORE INTO ietf_dt_person_email VALUES (?, ?, ?, ?, ?, ?)"
                db_cursor.execute(sql, val)

            try:
                if msg["to"] is not None:
                    try:
                        for to_name, to_addr in getaddresses([msg["to"]]):
                            sql = f"INSERT INTO ietf_ma_messages_to VALUES (?, ?, ?, ?)"
                            db_cursor.execute(sql, (None, tot_count, to_name, fixaddr(to_addr)))
                            # Many of the "To:" addresses are malformed.
                            # It's not clear it's useful to add them to
                            # the ietf_dt_person_email table.
                            #
                            # if has_dt_tables and to_addr is not None:
                            #     val = (0, fixaddr(to_addr), f"mailarchive", None, 0, parsed_date);
                            #     sql = f"INSERT or IGNORE INTO ietf_dt_person_email VALUES (?, ?, ?, ?, ?, ?)"
                            #     db_cursor.execute(sql, val)
                    except:
                        print(f"ERROR: cannot parse \"To:\" header for {msg_path}")
            except:
                print(f"ERROR: malformed \"To:\" header for {msg_path}")

            try:
                if msg["cc"] is not None:
                    try:
                        for cc_name, cc_addr in getaddresses([msg["cc"]]):
                            sql = f"INSERT INTO ietf_ma_messages_cc VALUES (?, ?, ?, ?)"
                            db_cursor.execute(sql, (None, tot_count, cc_name, fixaddr(cc_addr)))
                            # Many of the "Cc:" addresses are malformed.
                            # It's not clear it's useful to add them to
                            # the ietf_dt_person_email table.
                            #
                            # if has_dt_tables and cc_addr is not None:
                            #     val = (0, fixaddr(cc_addr), f"mailarchive", None, 0, parsed_date);
                            #     sql = f"INSERT or IGNORE INTO ietf_dt_person_email VALUES (?, ?, ?, ?, ?, ?)"
                            #     db_cursor.execute(sql, val)
                    except:
                        print(f"ERROR: cannot parse \"Cc:\" header for {msg_path}")
            except:
                print(f"ERROR: malformed \"Cc:\" header for {msg_path}")
    db_connection.commit()


print("  Vacuuming database")
db_connection.execute('VACUUM;') # Don't force fsync on the file between writes

print(f"  Could not parse header for {err_count} messages out of {tot_count}")

