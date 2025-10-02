from typing import Optional, List
from pydantic import BaseModel, Field

class CreateJobRequest(BaseModel):
    task_type: str
    # callback_url: str
    job_id: str
    task_id: str
    access_token: str
    advertiser_id: str
    store_id: str
    start_date: str
    end_date: str
    advertiser_name: Optional[str] = None
    store_name: Optional[str] = None
    
    # --- Thông tin cho việc ghi Sheet ---
    spreadsheet_id: str
    sheet_name: str
    is_overwrite: bool = False
    is_first_chunk: bool = False
    selected_fields: List[str] = Field(default_factory=list)