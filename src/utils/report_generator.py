from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)

def generate_report(start_time, end_time, row_count):
    """
    Generate pipeline execution report
    """

    duration = end_time - start_time

    report = f"""
    ================= PIPELINE REPORT =================
    Start Time   : {start_time}
    End Time     : {end_time}
    Duration     : {duration}
    Rows Processed : {row_count}
    Status       : SUCCESS
    ==================================================
    """

    logger.info(report)