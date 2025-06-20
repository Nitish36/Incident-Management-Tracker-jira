import requests
from requests.auth import HTTPBasicAuth
import gspread
from gspread_dataframe import set_with_dataframe
import os
import pandas as pd
import json

def push_data():
    email = 'thanujakedila@gmail.com'
    api_token = os.getenv("JIRA_SECRET")  # Get Jira API token from secret
    server = 'https://nitish36.atlassian.net'

    jql = 'project = "IMT" AND created >= -30d'

    url = f"{server}/rest/api/3/search"

    params = {
        "jql": jql,
        "maxResults": 100,
        "fields": "key,issuetype,customfield_10103,customfield_10104,statusCategory,created,priority,assignee,summary"
    }

    headers = {
        "Accept": "application/json"
    }

    response = requests.get(
        url,
        headers=headers,
        params=params,
        auth=HTTPBasicAuth(email, api_token)
    )

    data = response.json()

    issues = []
    print("Status Code:", response.status_code)
    print("Response Text:", response.text)
    for issue in data["issues"]:
        fields = issue["fields"]
        print(json.dumps(issue["fields"], indent=5))
        issues.append({
            "Jira Issue Key": issue["key"],
            "Jira Ticket Type": fields["issuetype"]["name"],
            "Description": fields["summary"],
            "Client ID": fields["customfield_10103"],
            "City": fields["customfield_10104"]["value"] if fields["customfield_10104"] else "",
            "Status": fields["statusCategory"]["name"],
            "Issue Date": fields["created"],
            "Priority": fields["priority"]["name"],
            "Assigned Person": fields["assignee"].get("emailAddress", "") if fields["assignee"] else "",

        })

    return issues

def write_df():
    issues = push_data()
    df = pd.DataFrame(issues)
    GSHEET_NAME = 'Incident Management Tracker'
    TAB_NAME = 'Dump'

    # Get Google credentials JSON string from env and parse
    gsheet_secret = os.getenv("GSHEET_SECRET")
    if not gsheet_secret:
        print("Google Sheets secret not found in environment.")
        return

    # Write the JSON to a temporary file
    credentialsPath = "temp_gsheet_credentials.json"
    with open(credentialsPath, "w") as f:
        f.write(gsheet_secret)

    try:
        gc = gspread.service_account(filename=credentialsPath)
        sh = gc.open(GSHEET_NAME)
        worksheet = sh.worksheet(TAB_NAME)
        set_with_dataframe(worksheet, df)
        print("Data loaded successfully!! Have fun!!")
        print(df)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if os.path.exists(credentialsPath):
            os.remove(credentialsPath)

def read_write():
    # Link= https://drive.google.com/file/d/127YpIp8SNvCEzkhm2zZ6sutMAi3kX3Oy/view?usp=drive_link
    url = "https://drive.google.com/uc?export=download&id=127YpIp8SNvCEzkhm2zZ6sutMAi3kX3Oy"
    dataset = pd.read_csv(url)
    dataset.columns = dataset.columns.str.strip()
    print("Columns:", dataset.columns.tolist())

    # Select the relevant columns
    data = {
        "Created": dataset["Created"],
        "Jira Issue Key": dataset["Jira Issue Key"],
        "Jira Ticket Type": dataset["Jira Ticket Type"],
        "Description": dataset["Description"],
        "Client ID": dataset["Client ID"],
        "City": dataset["City"],
        "Status": dataset["Status"],
        "Issue Date Only": dataset["Issue Date Only"],
        "Priority": dataset["Priority"],
        "Assigned Person": dataset["Assigned Person"],
        "Expected Resolved Date": dataset["Expected Resolved Date"],
        "Resolved Date": dataset["Resolved Date"],
        "Root Cause": dataset["Root Cause"],
        "SLA Breached": dataset["SLA Breached"],
        "Resolution Ageing": dataset["Resolution Ageing"],
        "Resolution TAT": dataset["Resolution TAT"],
        "Resolution Health": dataset["Resolution Health"],
        "Labels": dataset["Labels"],
        "Current Date": dataset["Current Date"],
        "Request MY": dataset["Request MY"],
        "Resolved MY": dataset["Resolved MY"],
    }
    final_data = pd.DataFrame(data)
    final_data['Issue Date Only'] = pd.to_datetime(final_data['Issue Date Only'], errors='coerce')
    final_data['Expected Resolved Date'] = pd.to_datetime(final_data['Expected Resolved Date'], errors='coerce')
    final_data['Resolved Date'] = pd.to_datetime(final_data['Resolved Date'], errors='coerce')
    final_data['Current Date'] = pd.to_datetime(final_data['Current Date'], errors='coerce')

    GSHEET_NAME = 'Incident Management'
    TAB_NAME = 'Incident'
    gsheet_secret = os.getenv("GSHEET_SECRET_KEY")
    if not gsheet_secret:
        print("Google Sheets secret not found in environment.")
        return

    # Write the JSON to a temporary file
    credentialsPath = "temp_gsheet_credentials.json"
    with open(credentialsPath, "w") as f:
        f.write(gsheet_secret)

    try:
        gc = gspread.service_account(filename=credentialsPath)
        sh = gc.open(GSHEET_NAME)
        worksheet = sh.worksheet(TAB_NAME)
        set_with_dataframe(worksheet, final_data)
        print("Data loaded successfully!! Have fun!!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if os.path.exists(credentialsPath):
            os.remove(credentialsPath)




write_df()
read_write()
