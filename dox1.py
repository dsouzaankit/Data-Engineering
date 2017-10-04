import pymysql.cursors
import json
import pandas as pd
from urllib.request import urlopen
import datetime
import timeit
import sys

#start code block timer
start_time = timeit.default_timer()

# Connect to the database
connection = pymysql.connect(host='34.196.124.246',
                             port=3316,
                             user='candidate',
                             password='Doximity',
                             db='data_engineer',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

url_stub = 'http://de-tech-challenge-api.herokuapp.com/api'
api_ver_str = 'v1' if len(sys.argv) <= 1 else sys.argv[1]
vdr_tbl = 'users'
url = '/'.join([url_stub, api_ver_str, vdr_tbl])
print("Vendor API url base: " + str(url))
curr_page = 1
vdata = json.load(urlopen(url))
max_pages = vdata['total_pages']
vdr_base = 0
#print(len(vdata['users']))
vdr_offset = round(max_pages * len(vdata['users']) / 11)
print("10% of total vendor rows: " + str(round(max_pages * len(vdata['users']) / 10)))
print("Max vendor rows processed per batch: " + str(vdr_offset + len(vdata['users'])))
curr_char = 'A'

try:

    with connection.cursor() as cursor:

        sql = "Select count(*) c from user "
        cursor.execute(sql)
        row = cursor.fetchone()
        dox_max = row['c']
        base = 0
        offset = round(dox_max / 11)
        print("10% of total dox records: " + str(round(dox_max / 10)))
        print("Max records per fetch: " + str(offset))

        excl_vdata = []
        excl_dox_data = []
        matches = []
        mixed_no_matches = []

        while curr_char <= 'Z':

            print("Currently matching last names beginning with: " + curr_char)

            vdata = []
            # fetch lastnames alphabetically
            vtemp_len = 0
            while vtemp_len <= vdr_offset and curr_page <= max_pages:
                page_str = '?page=' + str(curr_page)
                print("Curr vdr page: " + url + page_str)
                vtemp = json.load(urlopen(url + page_str))['users']
                vtemp = pd.DataFrame.from_dict(vtemp)
                # print(temp['lastname'][temp.shape[0] - 1])
                # we need to skip this page
                if vtemp['lastname'][vtemp.shape[0] - 1][0] < curr_char:
                    curr_page = curr_page + 1
                    continue
                # next char starts on this page
                elif vtemp['lastname'][vtemp.shape[0] - 1][0] > curr_char:
                    # filter next character data and break
                    vtemp = vtemp[vtemp['lastname'].str.startswith(curr_char)]
                    vdata.append(vtemp)
                    break
                # store current page and fetch next page
                else:
                    vdata.append(vtemp)
                    vtemp_len = vtemp_len + vtemp.shape[0]
                    curr_page = curr_page + 1

            # merge all dataframes contained in list
            vdata = pd.concat(vdata, axis = 0, ignore_index = True)

            # filter previous character data, if exists
            vdata = vdata[vdata['lastname'].str.startswith(curr_char)]
            #print(str(vdata.shape[0]))

            dox_data = []
            base = 0
            while True:

                print("Matching dox rows above: " + str(base))
                # query alphabetically
                sql = "select lastname, id from user " \
                      "where lastname like '" + curr_char + "%' " \
                      "order by lastname " \
                      "limit " + str(base) + ", " + str(offset)
                cursor.execute(sql)
                dox_data = cursor.fetchall()
                # check for records
                if (not dox_data):
                    break
                dox_data = pd.DataFrame.from_dict(dox_data)
                print("Current dox batch count for character " + curr_char + ": " + str(dox_data.shape[0]))

                # get unique last names and join both
                u_vdata = vdata.drop_duplicates('lastname')
                # print(u_vdata.head(10))
                u_dox = dox_data.drop_duplicates('lastname')
                u_matches = pd.merge(u_vdata, u_dox, on = ['lastname'])
                # print(u_matches.head(10))
                # check if atleast some last names match
                if not u_matches.empty:

                    excl_vdata.append(vdata[~vdata['lastname'].isin(u_matches['lastname'])])
                    excl_dox_data.append(dox_data[~dox_data['lastname'].isin(u_matches['lastname'])])
                    # filter common last names
                    vdata = vdata[vdata['lastname'].isin(u_matches['lastname'])]
                    dox_data = dox_data[dox_data['lastname'].isin(u_matches['lastname'])]

                    # print(str(dox_data.head(20)))
                    # get firstname and specialty from dox db, using ids; vdr details already available
                    dox_data2 = []
                    base2 = 0
                    while True:

                        sql = "select id, lastname, firstname, specialty from user " \
                              "where id in (" + ','.join(map(str, dox_data['id'])) + ") " \
                              "limit " + str(base2) + ", " + str(offset)
                        # print(sql)
                        cursor.execute(sql)
                        temp = cursor.fetchall()
                        # check for records
                        if (not temp):
                            break
                        temp = pd.DataFrame.from_dict(temp)
                        # print(str(temp.head(10)))
                        dox_data2.append(temp)
                        base2 = base2 + offset

                    dox_data = pd.concat(dox_data2, axis = 0, ignore_index = True)
                    # print(str(dox_data.head(30)))

                    # join records again to find matches
                    temp = pd.merge(vdata, dox_data, how = 'outer', on = ['lastname', 'firstname', 'specialty'])
                    mixed_no_matches.append(temp[pd.isnull(temp['id_x']) | pd.isnull(temp['id_y'])])

                    # effectively an inner join
                    temp = temp[pd.notnull(temp['id_x']) & pd.notnull(temp['id_y'])]
                    # print(str(temp.head(10)))
                    matches.append(temp)

                else:

                    excl_vdata.append(vdata)
                    excl_dox_data.append(dox_data)

                base = base + offset

            #if loop broke before vdr_block ended, then process next character
            if vtemp_len <= vdr_offset:
                curr_char = chr(ord(curr_char) + 1)

        matches = pd.concat(matches, axis = 0, ignore_index = True)
        mixed_no_matches = pd.concat(mixed_no_matches, axis = 0, ignore_index = True)
        excl_vdata = pd.concat(excl_vdata, axis = 0, ignore_index = True)
        excl_dox_data = pd.concat(excl_dox_data, axis = 0, ignore_index = True)

        # remove duplicates
        mixed_no_matches = mixed_no_matches.drop_duplicates(['lastname', 'firstname', 'specialty'])
        excl_vdata = excl_vdata.drop_duplicates('id')
        excl_dox_data = excl_dox_data.drop_duplicates('id')

        #get remaining column names from dox db
        rem_cols = []
        sql = "desc user"
        cursor.execute(sql)
        results = cursor.fetchall()
        #print(str(results))
        for result in results:
            if result['Field'] not in ['firstname', 'lastname', 'specialty']:
                rem_cols.append(result['Field'])

        #print(str(rem_cols))
        #re-query db to obtain remaining details for redshift; vendor details alrdy avlbl
        remaining_dox_cols = []
        base = 0
        while True:

            print("Querying related columns for dox rows above: " + str(base))
            sql = "select " + ", ".join(rem_cols) + " from user " \
                  "where id in (" + ",".join(map(str, matches['id_y'])) + ") " \
                  "limit " + str(base) + ", " + str(offset)
            cursor.execute(sql)
            temp = cursor.fetchall()
            # check for records
            if (not temp):
                break
            temp = pd.DataFrame.from_dict(temp)
            remaining_dox_cols.append(temp)
            base = base + offset

        remaining_dox_cols = pd.concat(remaining_dox_cols, axis = 0, ignore_index = True)
        matches = pd.merge(matches, remaining_dox_cols, left_on = 'id_y', right_on = 'id')

        #for no matches, same logic as above
        # re-query db to obtain remaining details for redshift; vendor details alrdy avlbl
        remaining_dox_cols = []
        base = 0
        while True:

            print("Querying related columns for excl. dox rows above: " + str(base))
            sql = "select " + ", ".join(rem_cols) + " from user " \
                  "where id not in (" + ",".join(map(str, matches['id_y'])) + ") " \
                  "and lastname < '" + curr_char + "' " \
                  "limit " + str(base) + ", " + str(offset)
            cursor.execute(sql)
            temp = cursor.fetchall()
            # check for records
            if (not temp):
                break
            temp = pd.DataFrame.from_dict(temp)
            remaining_dox_cols.append(temp)
            base = base + offset

        remaining_dox_cols = pd.concat(remaining_dox_cols, axis = 0, ignore_index = True)

        #remove redundancies
        mixed_no_matches = pd.merge(mixed_no_matches, remaining_dox_cols, left_on = 'id_y', right_on = 'id')
        excl_dox_data = pd.merge(excl_dox_data, remaining_dox_cols, on = 'id')
        excl_dox_data = excl_dox_data[~excl_dox_data['id'].isin(mixed_no_matches['id_y'])]
        excl_vdata = excl_vdata[~excl_vdata['id'].isin(matches['id_x'])]
        excl_vdata = excl_vdata[~excl_vdata['id'].isin(mixed_no_matches['id_x'])]

        excl_vdata = excl_vdata.rename(columns={'last_active_date': 'last_active_date_x', 'id': 'id_x'})
        excl_dox_data = excl_dox_data.rename(columns={'last_active_date': 'last_active_date_y', 'id': 'id_y'})

        mixed_no_matches = pd.concat([mixed_no_matches, excl_vdata, excl_dox_data], axis = 0)

        cnsl_data = pd.concat([matches, mixed_no_matches], axis = 0)

        #create source column for identifying record source
        cnsl_data.loc[cnsl_data.id_y.isnull(), 'source'] = 'Vendor'
        cnsl_data.loc[cnsl_data.id_x.isnull(), 'source'] = 'Doximity'
        cnsl_data.loc[cnsl_data.id_y.notnull() & cnsl_data.id_x.notnull(), 'source'] = 'Common'

        # replace NaNs with dummy dates
        cnsl_data.loc[cnsl_data.last_active_date_y.isnull(), 'last_active_date_y'] = datetime.datetime(1900, 1, 1)
        cnsl_data.loc[cnsl_data.last_active_date_x.isnull(), 'last_active_date_x'] = datetime.datetime(1900, 1, 1)

        # Use datetime with a day precision
        cnsl_data['last_active_date_x'] = cnsl_data['last_active_date_x'].astype('datetime64[D]')
        cnsl_data['last_active_date_y'] = cnsl_data['last_active_date_y'].astype('datetime64[D]')
        #assumption
        curr_date = datetime.datetime(2017, 2, 2)
        diff = datetime.timedelta(days = 30)

        #verify time interval between curr date and date_last_active <= 30 days
        cnsl_data['is_active_vdr'] = cnsl_data['last_active_date_x'].apply(
            lambda x: 'Y' if curr_date - x <= diff else 'N')

        cnsl_data['is_active_dox'] = cnsl_data['last_active_date_y'].apply(
            lambda x: 'Y' if curr_date - x <= diff else 'N')

        #Tried sorting and selectively updating statuses but code runtime was significantly higher

        #sort by date and update user activity statuses within last 30 days
        # cnsl_data['is_active_vdr'] = 'N'
        # cnsl_data = cnsl_data.sort_values('last_active_date_x', ascending = False)
        # cell = 0
        # dt_index = cnsl_data.columns.get_loc('last_active_date_x')
        # act_index = cnsl_data.columns.get_loc('is_active_vdr')
        # rows = cnsl_data.shape[0]
        # for cell in range(0, rows - 1):
        #     if curr_date - cnsl_data.iloc[cell, dt_index] <= diff:
        #         cnsl_data.iloc[cell, act_index] = 'Y'
        #     else:
        #         break
        #
        # print("ch6")
        # cnsl_data['is_active_dox'] = 'N'
        # cnsl_data = cnsl_data.sort_values('last_active_date_y', ascending = False)
        # cell = 0
        # dt_index = cnsl_data.columns.get_loc('last_active_date_y')
        # act_index = cnsl_data.columns.get_loc('is_active_dox')
        # rows = cnsl_data.shape[0]
        # for cell in range(0, rows - 1):
        #     if curr_date - cnsl_data.iloc[cell, dt_index] <= diff:
        #         cnsl_data.iloc[cell, act_index] = 'Y'
        #     else:
        #         break

        cnsl_data = cnsl_data.drop('id', axis = 1)
        cnsl_data = cnsl_data.rename(columns={'last_active_date_x': 'last_active_vdr', 'last_active_date_y': 'last_active_dox'})
        cnsl_data = cnsl_data.rename(columns={'id_x': 'id_vdr', 'id_y': 'id_dox'})

        #stop code block timer
        elapsed = timeit.default_timer() - start_time
        print("Script run time: " + str(elapsed))

        print("Output columns: " + str(list(cnsl_data)))
        print("Total Vdr excl. users: \n" + str(cnsl_data[pd.isnull(cnsl_data['id_dox'])].shape[0]))
        print("Total Dox excl. users: \n" + str(cnsl_data[pd.isnull(cnsl_data['id_vdr'])].shape[0]))
        print("Total users: " + str(cnsl_data.shape[0]))
        print("Common users: " + str(cnsl_data[pd.notnull(cnsl_data['id_vdr']) & pd.notnull(cnsl_data['id_dox'])].shape[0]))
        print("Top 10 common users: \n" + str(cnsl_data[pd.notnull(cnsl_data['id_dox']) & pd.notnull(cnsl_data['id_vdr'])].head(10)))
        print("Top 10 Vdr excl. users: \n" + str(cnsl_data[pd.isnull(cnsl_data['id_dox'])].head(10)))
        print("Top 10 Dox excl. users: \n" + str(cnsl_data[pd.isnull(cnsl_data['id_vdr'])].head(10)))

        cmn_smp = cnsl_data[pd.notnull(cnsl_data['id_dox']) & pd.notnull(cnsl_data['id_vdr'])].head(10).to_json(orient = "records")
        vdr_smp = cnsl_data[pd.isnull(cnsl_data['id_dox'])].head(10).to_json(orient = "records")
        dox_smp = cnsl_data[pd.isnull(cnsl_data['id_vdr'])].head(10).to_json(orient = "records")
        with open('records.txt', 'w') as f:
            f.write(cmn_smp + "\n")
            f.write(vdr_smp + "\n")
            f.write(dox_smp + "\n")
        print("Wrote top 10 matches to records.txt, in json format")

finally:
    connection.close()

#script runtime: 237s, max. memory usage: 400MB