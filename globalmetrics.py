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
        
        self.active_editors = {proj: {} for proj in projects}
        self.newly_registered = {proj: {} for proj in projects}
        self.uploaded_media = {'commonswiki': {user: [] for user in cohort}}
        self.absolute_bytes = {proj: {user: 0 for user in cohort} for proj in projects}
        self.edited_articles_list = {proj: {user: [] for user in cohort} for proj in projects}
        self.number_of_edits = {proj: {user: 0 for user in cohort} for proj in projects}

        start_minus_30_days = self.starttime.replace(days=-7)
        start_minus_30_days = start_minus_30_days.format('YYYYMMDDHHmmss')
        start = self.starttime.format('YYYYMMDDHHmmss')
        end = self.endtime.format('YYYYMMDDHHmmss')

        for project in projects:

            # Active editors

            q = ("select rev_user_text, count(*) from revision_userindex "
                 "where rev_timestamp >= {0} and rev_user_text in {1} "
                 "group by rev_user_text;").format(start_minus_30_days, tuple(self.cohort))
            q = self.sql.query(project, q, None)

            for row in q:
                user = row[0].decode('utf-8')
                if row[1] >= 5:
                    self.active_editors[project][user] = True
                else:
                    self.active_editors[project][user] = False

            # Newly registered users

            q = self.sql.query(project, "select user_name, user_registration from user where user_name in {0};".format(tuple(self.cohort)), None)
            
            # Taking advantage of the one user table query to validate usernames
            q_usernames = [x[0].decode('utf-8') for x in q]
            for user in cohort:
                if user not in q_usernames:
                    raise ValueError("Invalid username: " + user)

            for row in q:
                user = row[0].decode('utf-8')
                print(user)
                try:
                    reg = row[1].decode('utf-8')
                except AttributeError:
                    self.newly_registered[project][user] = False  # yo account so old it lacks a registration date
                    continue
                if reg >= start_minus_30_days:
                    self.newly_registered[project][user] = True
                else:
                    self.newly_registered[project][user] = False

            # Several edit-related metrics

            q1 = ("select rev_user_text, page_title, rev_len, rev_parent_id, "
                  "rev_id, page_namespace from revision_userindex "
                  "join page on rev_page = page_id "
                  "where rev_timestamp >= {0} "
                  "and rev_timestamp <= {1} "
                  "and rev_user_text in {2};").format(start, end, tuple(self.cohort))
            q1 = self.sql.query(project, q1, None)
                        
            # Absolute bytes
            
            prev_lengths = {}
            if len(q1) > 0:
                parents = {x[4]: x[3] for x in q1}
            
                q2 = ("select rev_id, rev_len from revision_userindex "
                      "where rev_id in {0}").format(tuple(parents.values()))
                q2 = self.sql.query(project, q2, None)
            
                prev_lengths = {x[0]: x[1] for x in q2}
            
                for revision, parent in parents.items():
                    if parent in [0, None]:
                        prev_lengths[parent] = 0

            # Combining the two queries above to calculate revision sizes
            # 0 = user; 1 = pagename; 2 = absolute value bytes contributed, 3 = namespace
            q = [[x[0].decode("utf-8"), \
                  x[1].decode("utf-8"), \
                  abs(x[2] - prev_lengths[x[3]]), \
                  x[5]] \
                  for x in q1 if x[2] != None and x[3] in prev_lengths]
            
            for row in q:
                self.absolute_bytes[project][row[0]] += row[2]
                    
                # Articles edited or improved (and ns 0 edit count)
                
                if row[3] == 0:  # Metric only applies to articles
                    self.number_of_edits[project][row[0]] += 1
                    if row[1] not in self.edited_articles_list[project][row[0]]:
                        self.edited_articles_list[project][row[0]].append(row[1])

        # Media uploaded to Commons
        
        q = self.sql.query("commonswiki", "select rev_user_text, page_title from revision_userindex join page on rev_page = page_id where page_namespace = 6 and rev_parent_id = 0 and rev_timestamp >= {0} and rev_timestamp <= {1} and rev_user_text in {2};".format(start, end, tuple(self.cohort)), None)

        for row in q:
            user = row[0].decode('utf-8')
            file = row[1].decode('utf-8')
            if user in self.uploaded_media['commonswiki']:
                self.uploaded_media['commonswiki'][user].append(file)
            else:
                self.uploaded_media['commonswiki'][user] = [file]