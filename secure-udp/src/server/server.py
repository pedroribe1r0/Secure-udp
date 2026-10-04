import socket
from threading import Thread, Lock
from pathlib import Path
from ..definitions.package import Package
from ..definitions.package import MessageType
from ..timeout.timeout import MessageTimeout
from ..shared.checksum import verify_package_checksum
from ..file.file_manager import FileManager
from .path_parser import parse_request
from ..shared.constants import MAX_RETRIES, PACKAGE_SIZE

class Server:
    connections: dict
    sock: socket.socket | None
    addr: tuple[str, int]
    file_manager: FileManager

    def __init__(self, base_dir: str | Path | None = None):
        if base_dir is None:
            base_dir = Path(__file__).resolve().parents[2] / "files" / "server_files"
        self.connections = {}
        self.sock = None
        self.addr = ("0.0.0.0", 5000)
        self.file_manager = FileManager(base_dir)

    def bind(self, addr: tuple[str, int]):
        ip, port = addr
        if not ip or not port:
            return
        self.addr = addr

    def start_server(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(self.addr)
        print(f"[SERVIDOR] Escutando em {self.addr}", flush=True)
        
        while True:
            data, addr = self.sock.recvfrom(PACKAGE_SIZE + 1)
            Thread(
                target=self._handle_connection,
                args=(data, addr)
            ).start()

    def _close_connection(self, addr):
        if not(addr in self.connections):
            return
        connection = self.connections[addr]
        
        connection["timer"].cancel()
        self.connections.pop(addr)
        print(f"[SERVIDOR {addr}] Transferência encerrada.", flush=True)

    def _handle_connection(self, data: bytes, addr: tuple[str, int]):
        if self.sock is None or not 9 <= len(data) <= PACKAGE_SIZE:
            return
        try:
            package = Package.from_bytes(data)
        except ValueError:
            return
        if not verify_package_checksum(package):
            return
    
        connection = self.connections.setdefault(addr, {"lock": Lock()})
        with connection["lock"]:
            if self.connections.get(addr) is not connection:
                return
            try:
                match package.headers.type:
                    case MessageType.DATA:
                        print(f"[SERVIDOR {addr}] Pedido: {package.payload.decode(errors='replace')}", flush=True)
                        self._handle_new_request(package, addr)
                    case MessageType.ACK:
                        self._handle_ack_message(package, addr)
                    case MessageType.EOF:
                        if ("timer" in connection and not connection["current_data"]
                                and package.headers.sequence_number == connection["sequence"]):
                            print(f"[SERVIDOR {addr}] EOF confirmado: seq={package.headers.sequence_number}", flush=True)
                            self._close_connection(addr)
                    case MessageType.ERR:
                        if "timer" in connection:
                            self._close_connection(addr)
            finally:
                if "timer" not in connection and self.connections.get(addr) is connection:
                    self.connections.pop(addr)

    def _send_error_message(self, message: bytes, addr: tuple[str, int]):
        if self.sock is None:
            return

        print(f"[SERVIDOR {addr}] ERRO: {message.decode(errors='replace')}", flush=True)
        self.sock.sendto(
            Package.create(payload=message, sequence=0, type=MessageType.ERR).to_bytes(),
            addr
        )

    def _send_data_or_eof(self, data: bytes, sequence: int, addr: tuple[str, int]):
        if self.sock is None:
            return

        package = Package.create(
            payload=data or b"End of file",
            sequence=sequence,
            type=MessageType.DATA if data else MessageType.EOF,
        )
        print(f"[SERVIDOR {addr}] Enviando {package.headers.type.name} seq={sequence}, dados={len(data)} bytes", flush=True)
        self.sock.sendto(package.to_bytes(), addr)

    def _handle_new_request(self, package: Package, addr: tuple[str, int]):
        if self.sock is None:
            return
        if "timer" in self.connections[addr]:
            return

        try:
            file_path = parse_request(package.payload, self.file_manager.base_dir)
        except ValueError as err:
            self._send_error_message(message=err.__str__().encode("utf-8"), addr=addr)
            return

        timer = MessageTimeout(interval=2.0, addr=addr)
        sequence = package.headers.sequence_number
        
        data = self.file_manager.read_chunk(filename=file_path, position=sequence)

        self.connections[addr].update({
            "file_path": file_path, 
            "timer": timer,
            "current_data": data,
            "sequence": sequence,
            "send_counter": 0
        })

        timer.reset_timer(
            action=self.timeout,
            sequence=sequence
        )

        self._send_data_or_eof(data, sequence, addr)

    def _handle_ack_message(self, package:Package, addr: tuple[str, int]):
        if "timer" not in self.connections.get(addr, {}):
            self._send_error_message(message=b"File not requested yet", addr=addr)
            return

        connection = self.connections[addr]
        sequence = package.headers.sequence_number

        print(f"[SERVIDOR {addr}] ACK recebido: {sequence}", flush=True)
        expected_ack = connection["sequence"] + len(connection["current_data"])
        if not connection["current_data"] or sequence != expected_ack:
            print(f"[SERVIDOR {addr}] ACK ignorado; aguardando confirmação do envio atual.", flush=True)
            return

        connection["sequence"] = sequence
        connection["send_counter"] = 0
        data = self.file_manager.read_chunk(filename=connection["file_path"], position=sequence)
        connection["current_data"] = data

        connection["timer"].reset_timer(
            action=self.timeout,
            sequence=sequence
        )

        self._send_data_or_eof(data, sequence, addr)

    def timeout(self, addr: tuple[str, int], seq: int):
        connection = self.connections.get(addr)
        if connection is None:
            return
        with connection["lock"]:
            if self.connections.get(addr) is not connection or "timer" not in connection:
                return

            if seq != connection["sequence"]:
                return
            connection["send_counter"] += 1
            print(f"[SERVIDOR {addr}] TIMEOUT seq={seq}, contador={connection['send_counter']}/{MAX_RETRIES}", flush=True)
        
            if connection["send_counter"] >= MAX_RETRIES:
                if connection["current_data"]:
                    self._send_error_message(message=b"Connection timeout", addr=addr)
                self._close_connection(addr=addr)
                return

            data = connection["current_data"]

            connection["timer"].reset_timer(
                action=self.timeout,
                sequence=connection["sequence"]
            )

            self._send_data_or_eof(data, seq, addr)
        