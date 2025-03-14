from pydantic import BaseModel
from typing import List

class CityAllocationRequest(BaseModel):
    cities: List[str]
