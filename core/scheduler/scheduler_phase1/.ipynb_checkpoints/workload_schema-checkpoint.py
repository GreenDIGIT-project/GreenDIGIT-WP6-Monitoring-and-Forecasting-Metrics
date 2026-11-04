# Scheduler request schema

from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import Optional, Literal

QoSLevel = Literal["LOW", "MEDIUM", "HIGH"]

SchedulingMode = Literal["carbon", "energy", "qos","balanced"]

# Temp examples
SCHEDULING_HORIZONS = {
    "carbon": timedelta(hours=12),     
    "energy": timedelta(hours=5),     
    "qos": timedelta(hours=2),         
    "balanced": timedelta(hours=0.5),  
}
# Workload request
@dataclass
class WorkloadRequest:
    
    data_amount_mb: float
    bandwidth_req_mbps: float

    min_qos: float  # [0, 1]

    # Optional deadline (if None -> horizon will be applied later)
    deadline: Optional[datetime] = None
    # Default scheduling policy
    mode: SchedulingMode = "balanced"

    # Optional future extension
    priority: Optional[int] = None

    request_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def finalize(self, scheduling_horizon: Optional[datetime] = None):
    
        if self.deadline is None:
            if scheduling_horizon is None:
                raise ValueError(
                    "Either deadline or scheduling_horizon must be provided"
                )
    
            # -------------------------------------------------
            # FORCE UTC CONSISTENCY (IMPORTANT FOR VALIDATION)
            # -------------------------------------------------
            if scheduling_horizon.tzinfo is None:
                scheduling_horizon = scheduling_horizon.replace(tzinfo=timezone.utc)
    
            self.deadline = scheduling_horizon

    # =====================================================
    # VALIDATION
    # =====================================================
    def validate(self):
        if self.data_amount_mb <= 0:
            raise ValueError("data_amount_mb must be > 0")

        if self.bandwidth_req_mbps <= 0:
            raise ValueError("bandwidth_req_mbps must be > 0")

        if not (0.0 <= self.min_qos <= 1.0):
            raise ValueError("min_qos must be in [0, 1]")

        if self.deadline is not None:
        
            deadline = self.deadline
            request_time = self.request_time
        
            # normalize if needed
            if deadline.tzinfo is None:
                deadline = deadline.replace(tzinfo=timezone.utc)
        
            if request_time.tzinfo is None:
                request_time = request_time.replace(tzinfo=timezone.utc)
        
            if deadline < request_time:
                raise ValueError("deadline cannot be earlier than request_time")

    # =====================================================
    # DICT EXPORT (FOR SCHEDULER PIPELINE)
    # =====================================================
    def to_dict(self):
        return {
            "data_amount_mb": self.data_amount_mb,
            "bandwidth_req_mbps": self.bandwidth_requirement_mbps,
            "min_qos": self.min_qos,
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "mode": self.mode,
            "priority": self.priority,
            "request_time": self.request_time.isoformat(),
        }