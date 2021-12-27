import dataclasses
import datetime
import json
import typing


@dataclasses.dataclass(frozen=True)
class Event:
    operation: typing.Literal["insert", "update", "delete"]
    extra: typing.Optional[typing.Any]
    received_at: datetime.datetime

    @staticmethod
    def load(payload: typing.Union[str, bytes]) -> "Event":
        p = json.loads(payload)
        return Event(
            operation=p["operation"],
            received_at=datetime.datetime.now(),
            extra=p.get("extra"),
        )
