from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict
from nova_framework.utils.pipeline_metrics import PipelineMetrics


@dataclass
class PipelineStats:
    """
    Tracks operational metrics and throughput statistics for a single pipeline execution.
    Intended to be embedded in PipelineContext and updated across modules.
    """

    rows_read: int = 0
    duplicate_rows: int = 0
    violation_rows: int = 0
    clean_rows: int = 0
    merged_rows: int = 0
    rejected_rows: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    merge_details: Dict[str, int] = field(default_factory=dict)

    def log_stat(self, metric: str, value: int):
        """
        Update an existing stat or add a new one dynamically.
        """
        if hasattr(self, metric):
            setattr(self, metric, getattr(self, metric) + value)
        else:
            self.merge_details[metric] = self.merge_details.get(metric, 0) + value

    def finalize(self, process_queue_id: int):
        """
        Mark pipeline completion and emit summary PipelineMetrics
        """
        self.end_time = datetime.now()
        PipelineMetrics.log_metrics(
            process_queue_id=process_queue_id,
            metrics_dict=self.summary()
        )

    def summary(self) -> Dict[str, int]:
        """
        Returns all tracked metrics as a dictionary for persistence or reporting.
        """
        stats = {
            "rows_read": self.rows_read,
            "duplicate_rows": self.duplicate_rows,
            "violation_rows": self.violation_rows,
            "clean_rows": self.clean_rows,
            "merged_rows": self.merged_rows,
            "rejected_rows": self.rejected_rows,
        }
        stats.update(self.merge_details)
        return stats