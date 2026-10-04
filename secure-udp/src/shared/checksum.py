from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..definitions.package import Package


def calculate_checksum(data: bytes) -> int:
    if len(data) % 2 != 0:
        data += b"\x00"

    total = 0

    for i in range(0, len(data), 2):
        word = int.from_bytes(data[i:i+2], "big")

        total += word

        total = (total & 0xFFFF) + (total >> 16)

    return ~total & 0xFFFF

def verify_package_checksum(package: "Package") -> bool:
    return calculate_checksum(package.payload) == package.headers.checksum
