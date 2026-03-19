import json
from datetime import datetime
import os


def generate_report(pipeline_name, records_read, records_written, status):

    report = {
        "pipeline_name": pipeline_name,
        "run_time": str(datetime.now()),
        "records_read": records_read,
        "records_written": records_written,
        "status": status
    }

    # create report file name
    report_file = f"reports/pipeline_report_{datetime.now().date()}.json"

    # write report
    with open(report_file, "w") as f:
        json.dump(report, f, indent=4)

    print(f"Report generated at: {report_file}")