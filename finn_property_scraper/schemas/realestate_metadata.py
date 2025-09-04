from pydantic.v1 import BaseModel


class RealestateMetadata(BaseModel):
    url: str
    category: str
    finn_id: str

    def __eq__(self, other):
        return self.finn_id == other.finn_id

    def __hash__(self):
        return hash(self.finn_id)