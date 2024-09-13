import dataclasses


@dataclasses.dataclass
class Visit:
    visit_id: int
    event_time: int
    browser: str