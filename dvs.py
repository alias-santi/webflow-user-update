import requests
import time
import datetime
import csv
import argparse

def load_csv():
    results = []

    with open(CSV_FILE, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            results.append(row)
    
    return results

def webflow_all_users():
    params = {
        'limit' : 100,
        'offset' : 0
    }
    
    users_endpoint = f"{WEBFLOW_API}/sites/{SITE_ID}/users"
    print(users_endpoint)
    all_users = []

    while True:
        print(f"Fetching Users.... Params: {params}")
        reponse = requests.get(users_endpoint, headers=HEADERS, params=params)
        if reponse.status_code != 200:
            raise Exception(f"ERROR: Fetching users. Text: {reponse.text}")
        
        data = reponse.json()
        all_users.extend(data['users'])
        params['offset'] += params['limit']

        check_rate_limit(reponse.headers)

        if len(all_users) >= data['total']:
            print("Collected All users from Webflow!")
            break
    
    return all_users

def check_rate_limit(header):
    remaining_rate = header['X-Ratelimit-Remaining']
    date = header['Date']
    if remaining_rate == '1':
        format_string = '%a, %d %b %Y %H:%M:%S %Z'
        date_object = datetime.datetime.strptime(date, format_string)
        sleep_time = (60 - date_object.second) + (1000000 - date_object.microsecond) / 1000000
        print(f"Webflow Rate Limit Throttle, sleeping for {sleep_time}...")
        return time.sleep(sleep_time)

def format_users(users):
    formatted = {}
    print(f"Extracting User Emails & IDs...")
    for user in users:
        formatted[f"{user['data']['email']}"] = user['_id']
    
    return formatted

def check_course_members_exist(course_members, webflow_users):
    missing_users = []
    existing_users = {}

    webflow_list = webflow_users.keys()

    for member in course_members:
        if member not in webflow_list:
            missing_users.append(member)
            continue

        existing_users[f'{member}'] = webflow_users[f'{member}']
    
    return {
        "missing_users": missing_users,
        "existing_users": existing_users
    }

def webflow_get_user(user_id):    
    endpoint = f"{WEBFLOW_API}/sites/{SITE_ID}/users/{user_id}"

    print(f"Fecthing user, {user_id}")
    reponse = requests.get(endpoint, headers=HEADERS)
    if reponse.status_code != 200:
        raise Exception(f"ERROR: Fetching user. Text: {reponse.text}")
    
    check_rate_limit(reponse.headers)
        
    return reponse.json()

def webflow_update_user_groups(user_id):
    endpoint = f"{WEBFLOW_API}/sites/{SITE_ID}/users/{user_id}/accessgroups"

    payload = { "accessGroups": ["course-members"] }

    reponse = requests.put(endpoint, headers=HEADERS, json=payload)
    if reponse.status_code != 200:
        raise Exception(f"ERROR: Unable to update User Groups. Text: {reponse.text}")
    
    return

def process_users(users):
    unverified = []
    success = []
    error = []

    for k, v in users.items():
        try:
            print(f"Processing {k}...")
            user = webflow_get_user(v)
        except Exception as e:
            error.append({
                "email": k,
                "step": "fetch_user",
                "msg": e
            })
            continue

        if user['status'] == "unverified":
            unverified.append(k)
            continue
        
        try:
            print(f"Adding {k} to course-members group...")
            group_add = webflow_update_user_groups(v)
        except Exception as e:
            error.append({
                "email": k,
                "step": "add_group",
                "msg": repr(e)
            })
            continue
        
        success.append(k)
    
    return {
        "unverified": unverified,
        "success": success,
        "error": error
    }


def main():

    print(f"Fetching Course Members List...")
    course_members_raw = load_csv()
    course_members = [x['Email 1'] for x in course_members_raw]

    print(f"Fetching All Webflow Users..")
    webflow_users = webflow_all_users()
    webflow_users_formmated = format_users(webflow_users)
    
    print(f"Checking Course Members against Webflow list..")
    member_check = check_course_members_exist(course_members, webflow_users_formmated)

    print(f"\nProcessing Users...")
    processing = process_users(member_check['existing_users'])
    
    # Some dirty prints for different conditions and events etc. Loggers should always be used..
    print(f"\nThe following Users no not exist in Webflow...\n{member_check['missing_users']}")
    print(f"\nThe following Users are not verfied in Webflow...\n{processing['unverified']}")
    print(f"\nThe following Users were successfully added to course-members in Webflow...\n{processing['success']}")
    print(f"\nThe following Users were NOT added to course-members in Webflow...\n{processing['error']}")

    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--site-id", help="Webflow Site ID", required=True)
    parser.add_argument("--token", help="Webflow Bearer Token", required=True)
    parser.add_argument("--members-file", help="CSV file containing List of Users to Process")
    args = parser.parse_args()

    TOKEN = args.token
    SITE_ID = args.site_id
    HEADERS = {
        "accept": "application/json",
        "authorization": f"Bearer {TOKEN}",
        "content-type": "application/json"
    }
    CSV_FILE = args.members_file
    WEBFLOW_API = 'https://api.webflow.com'


    main()
