import os
import logging

log_dir = "/home/tusharlmbt/Batch_data_pipeline_orders_project1/logs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(log_dir, "pipeline.log"),
    level=logging.INFO
)

logger = logging.getLogger(__name__)
