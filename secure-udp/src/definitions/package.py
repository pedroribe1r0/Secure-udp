from dataclasses import dataclass
from enum import Enum
from ..shared.checksum import calculate_checksum

class MessageType(Enum):
    DATA = "D"
    ACK = "A"
    ERR = "E"
    EOF = "F"


@dataclass()
class Headers:
    checksum: int
    sequence_number: int
    type: MessageType


@dataclass()
class Package:
    headers: Headers
    payload: bytes

    def to_bytes(self) -> bytes:
        checksum = self.headers.checksum.to_bytes(4, "big")
        sequence = self.headers.sequence_number.to_bytes(4, "big")
        message_type = self.headers.type.value.encode("ascii")

        return checksum + sequence + message_type + self.payload

    @classmethod
    def from_bytes(cls, data: bytes) -> "Package":
        checksum = int.from_bytes(data[0:4], "big")
        sequence = int.from_bytes(data[4:8], "big")

        message_type = MessageType(
            data[8:9].decode("ascii")
        )

        payload = data[9:]

        headers = Headers(
            checksum=checksum,
            sequence_number=sequence,
            type=message_type
        )

        return cls(
            headers=headers,
            payload=payload
        )

    @classmethod
    def create(cls, payload: bytes, sequence: int, type: MessageType):
        return Package(
            Headers(
                checksum=calculate_checksum(payload),
                sequence_number=sequence,
                type=type
            ),
            payload
        )

