from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class SimulatedWorkload:
    """
    Internal scheduler representation of a workload.
    This is NOT the API request object.
    """

    exec_id: str
    link_id: str

    src_node: str
    dst_node: str

    start_time: datetime
    data_amount_mb: float
    bandwidth_req_mbps: float

    # Phase 1 assumption: deterministic duration
    predicted_duration_s: float

    status: str = "SCHEDULED"

    # Computed field (updated during simulation)
    remaining_s: float = field(init=False)

    def __post_init__(self):
        self.remaining_s = self.predicted_duration_s

    def update_progress(self, elapsed_s: float):
        """
        Simulates progress inside a time window.
        (used for short horizon simulation only)
        """
        self.remaining_s = max(0.0, self.remaining_s - elapsed_s)

    def is_finished(self) -> bool:
        return self.remaining_s <= 0