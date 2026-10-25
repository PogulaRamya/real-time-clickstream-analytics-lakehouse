"""Explicit publication helper; run only after validating the workbook in Desktop."""
import argparse
import os
from pathlib import Path

def main():
    import tableauserverclient as TSC
    parser = argparse.ArgumentParser()
    parser.add_argument("workbook", type=Path)
    args = parser.parse_args()
    if not args.workbook.is_file() or args.workbook.suffix not in (".twb", ".twbx"):
        parser.error("Provide a validated .twb or .twbx file")
    auth = TSC.PersonalAccessTokenAuth(os.environ["TABLEAU_TOKEN_NAME"],
        os.environ["TABLEAU_TOKEN_SECRET"], site_id=os.environ["TABLEAU_SITE"])
    server = TSC.Server(os.environ["TABLEAU_SERVER"], use_server_version=True)
    with server.auth.sign_in(auth):
        item = TSC.WorkbookItem(project_id=os.environ["TABLEAU_PROJECT_ID"], name="Clickstream Lakehouse")
        # CreateNew avoids overwriting an existing dashboard accidentally.
        published = server.workbooks.publish(item, str(args.workbook), TSC.Server.PublishMode.CreateNew)
        print(f"Published workbook id: {published.id}")

if __name__ == "__main__":
    main()
