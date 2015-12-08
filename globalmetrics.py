# -*- coding: utf-8 -*-
"""
Creates an API for surfacing WMF global metrics
Copyright (C) 2015 James Hare
Licensed under MIT License: http://mitlicense.org
"""

import arrow
import tool_labs_utils

# (list of) Articles Edited During Event: 109850233
# (list of) Media Files Uploaded: 109850237
# Number of Articles Edited: 109850234
# Number of Edits Made: 109866023
# Number of Bytes Changed: 109850235
# Number of Media Files Uploaded: 109850236


class GlobalMetrics:
    def __init__(self, cohort, projects, starttime, endtime):
        self.cohort = cohort
        self.projects = projects
        self.starttime = starttime
        self.endtime = endtime

        self.sql = tool_labs_utils.WMFReplica()  # usage: GlobalMetrics.sql.query(db, sqlquery, values)
        
        self.active_editors = {}
        self.newly_registered = {}
        self.uploaded_media = {'commonswiki': {}}
        self.absolute_bytes = {}
        self.edited_articles_list = {}

        start_minus_30_days = self.starttime.replace(days=-30)
        start_minus_30_days = start_minus_30_days.format('YYYYMMDDHHmmss')
        start = self.starttime.format('YYYYMMDDHHmmss')
        end = self.endtime.format('YYYYMMDDHHmmss')

        for project in projects:

            # Active editors

            q = ("select rev_user_text, count(*) from revision_userindex "
                 "where rev_timestamp >= {0} and rev_user_text in {1} "
                 "group by rev_user_text;").format(start_minus_30_days, tuple(self.cohort))
            print(q)
            q = self.sql.query(project, q, None)
            self.active_editors[project] = {}

            for row in q:
                user = row[0].decode('utf-8')
                if row[1] >= 5:
                    self.active_editors[project][user] = True
                else:
                    self.active_editors[project][user] = False

            # Newly registered users
            
            self.newly_registered[project] = {}

            q = self.sql.query(project, "select user_name, user_registration from user where user_name in {0};".format(tuple(self.cohort)), None)

            for row in q:
                user = row[0].decode('utf-8')
                reg = row[1].decode('utf-8')
                if reg <= start_minus_30_days:
                    self.newly_registered[project][user] = True
                else:
                    self.newly_registered[project][user] = False

            # Absolute bytes
            
            self.absolute_bytes[project] = {}
            self.edited_articles_list[project] = {}

            q1 = ("select rev_user_text, page_title, rev_len, rev_parent_id, "
                  "rev_id from revision_userindex "
                  "join page on rev_page = page_id "
                  "where rev_timestamp >= {0} "
                  "and rev_timestamp <= {1} "
                  "and rev_user_text in {2};").format(start, end, tuple(self.cohort))
            q1 = self.sql.query(project, q1, None)
            
            prev_lengths = {}
            if len(q1) > 0:
                parents = {x[4]: x[3] for x in q1}
            
                q2 = ("select rev_id, rev_len from revision_userindex "
                      "where rev_id in {0}").format(tuple(parents.values()))
                q2 = self.sql.query(project, q2, None)
            
                prev_lengths = {x[0]: x[1] for x in q2}
            
                for revision, parent in parents.items():
                    if parent == 0:
                        prev_lengths[revision] = 0

            # Combining the two queries above to calculate revision sizes
            # 0 = user; 1 = pagename; 2 = absolute value bytes contributed
            q = [[x[0].decode("utf-8"), \
                  x[1].decode("utf-8"), \
                  abs(x[2] - prev_lengths[x[2]])] \
                 for x in q1]
            
            for row in q:
                if row[0] in self.absolute_bytes[project]:
                    self.absolute_bytes[project][row[0]] += row[2]
                else:
                    self.absolute_bytes[project][row[0]] = row[2]
                    
                # Articles edited or improved
                
                if row[0] in self.edited_articles_list[project]:
                    if row[1] not in self.edited_articles_list[project][row[0]]:
                        self.edited_articles_list[project][row[0]].append(row[1])
                else:
                    self.edited_articles_list[project][row[0]] = [row[1]]

            # For users not making any edits in the time period
            for user in self.cohort:
                if user not in self.absolute_bytes[project]:
                    self.absolute_bytes[project][user] = 0
                if user not in self.edited_articles_list[project]:
                    self.edited_articles_list[project][user] = []

        # Media uploaded to Commons
        
        q = self.sql.query("commonswiki", "select rev_user_text, page_title from revision_userindex join page on rev_page = page_id where page_namespace = 6 and rev_parent_id = 0 and rev_timestamp >= {0} and rev_timestamp <= {1} and rev_user_text in {2};".format(start, end, tuple(self.cohort)), None)

        for row in q:
            user = row[0].decode('utf-8')
            file = row[1].decode('utf-8')
            if user in self.uploaded_media['commonswiki']:
                self.uploaded_media['commonswiki'][user].append(file)
            else:
                self.uploaded_media['commonswiki'][user] = [file]
        
        # For users not making any uploads in the time period
        for user in self.cohort:
            if user not in self.uploaded_media['commonswiki']:
                self.uploaded_media['commonswiki'][user] = []