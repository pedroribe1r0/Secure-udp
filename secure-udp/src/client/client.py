import socket
from ..timeout.timeout import MessageTimeout
from ..definitions.package import Package, MessageType
from ..shared.constants import MAX_RETRIES, PACKAGE_SIZE, CHUNK_SIZE
from ..shared.checksum import verify_package_checksum
from ..file.file_manager import FileManager
from pathlib import Path

class Client:
    sock: socket.socket
    server_addr: tuple[str, int]
    timer: MessageTimeout
    expected_sequence_number: int
    file_manager: FileManager

    def __init__(self, server_addr: tuple[str, int], base_path: str | Path | None, discard_blocks: set[int] | None = None):
        if base_path is None:
            base_path = Path(__file__).resolve().parents[2] / "files" / "client_files"
        self.discard_blocks = set(discard_blocks or ())
        if any(block < 1 for block in self.discard_blocks):
            raise ValueError("Block numbers start at 1")
        self._discarded_blocks = set()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server_addr = server_addr
        
        self.timer = MessageTimeout(2.0, self.server_addr)
        self.expected_sequence_number = 0
        self.file_manager = FileManager(base_path)
        self.is_connected = False
        self._timeout_count = 0
        self._request_path = ""

    def _log(self, message: str):
        print(f"[{self.file_manager.base_dir.name} porta={self.sock.getsockname()[1]}] {message}", flush=True)

    def _clean_state(self):
        self.is_connected = False
        if self.timer:
            self.timer.cancel()
        self.expected_sequence_number = 0
        self._timeout_count = 0
        self.sock.settimeout(None)

    def _request_file(self, filepath: str):
        payload = f"GET {filepath}".encode("utf-8")
        package = Package.create(payload=payload, sequence=0, type=MessageType.DATA)
        
        self.sock.sendto(package.to_bytes(), self.server_addr)
        self._log(f"GET enviado: {filepath}")

    def _send_ack(self):
        self._log(f"Enviando ACK: próximo byte={self.expected_sequence_number}")
        self.sock.sendto(
            Package.create(b"", self.expected_sequence_number, MessageType.ACK).to_bytes(),
            self.server_addr
        )

    def _send_eof(self):
        self._log(f"Confirmando EOF: seq={self.expected_sequence_number}")
        self.sock.sendto(
            Package.create(b"File ended", self.expected_sequence_number, MessageType.EOF).to_bytes(),
            self.server_addr
        )

    def _handle_timeout(self, addr: tuple[str, int], sequence: int):
        if (not self.is_connected or addr != self.server_addr
                or sequence != self.expected_sequence_number):
            return

        self._timeout_count += 1
        self._log(f"TIMEOUT {self._timeout_count}/{MAX_RETRIES}; próximo byte={self.expected_sequence_number}")
        if self._timeout_count >= MAX_RETRIES:
            self.is_connected = False
            return

        if self.expected_sequence_number == 0:
            self._request_file(self._request_path)

        if self.is_connected:
            self.timer.reset_timer(self._handle_timeout, self.expected_sequence_number)

    def _handle_data_payload(self, package: Package):
        incoming_sequence = package.headers.sequence_number

        if self.expected_sequence_number != incoming_sequence:
            self._log(f"DATA duplicado/fora de ordem: seq={incoming_sequence}; não será gravado")
            return (b"", 0)

        self._log(f"DATA aceito: seq={incoming_sequence}, {len(package.payload)} bytes")
        self.expected_sequence_number += len(package.payload)
        if package.payload:
            self._timeout_count = 0

        return (package.payload, incoming_sequence)

    def _validate_incoming_data(self, data: bytes):
        is_length_valid = 9 <= len(data) <= PACKAGE_SIZE

        if not is_length_valid:
            return False
        try:
            package = Package.from_bytes(data)
        except ValueError:
            return False
        return verify_package_checksum(package)

    def _receive_file(self):
        EOF = False
        while not EOF:
            if not self.is_connected:
                raise TimeoutError("Maximum consecutive timeouts reached")
            try:
                data, addr = self.sock.recvfrom(PACKAGE_SIZE)
            except socket.timeout:
                continue
            if not self.is_connected:
                raise TimeoutError("Maximum consecutive timeouts reached")

            if not self._validate_incoming_data(data) or not addr == self.server_addr:
                yield (b"", 0)
                continue

            try:
                package = Package.from_bytes(data)
            except ValueError:
                yield (b"", 0)
                continue
         
            match package.headers.type:
                case MessageType.DATA:
                    block = package.headers.sequence_number // CHUNK_SIZE + 1
                    if block in self.discard_blocks and block not in self._discarded_blocks:
                        self._discarded_blocks.add(block)
                        self._log(f"PERDA SIMULADA: descartando bloco {block}, seq={package.headers.sequence_number}; sem ACK")
                        continue
                    yield self._handle_data_payload(package)

                case MessageType.EOF:
                    self._log(f"EOF recebido: seq={package.headers.sequence_number}")
                    EOF = True

                case MessageType.ERR:
                    raise RuntimeError(f"Servidor: {package.payload.decode('utf-8', errors='replace')}")
        
    def recvfrom(self, filename: str):
        if self.is_connected:
            raise Exception("There is a request in progress already")

    
        self.expected_sequence_number = 0
        self._timeout_count = 0
        self._discarded_blocks.clear()
        file_started = False
        self._request_path = filename if filename.startswith("/") else f"/{filename}"
        filename = self._request_path[1:]
        self.is_connected = True

        try:
            self.sock.settimeout(0.2)
            self._request_file(self._request_path)
            self.timer.reset_timer(self._handle_timeout, self.expected_sequence_number)
            for chunk, sequence in self._receive_file():
                if chunk:
                    if not file_started:
                        self.file_manager.write_chunk(filename, b"", 0)
                        file_started = True
                    self.file_manager.write_chunk(
                        filename=filename,
                        data=chunk,
                        position=sequence
                    )
                
                self._send_ack()
                self.timer.reset_timer(self._handle_timeout, self.expected_sequence_number)
            
            self._send_eof()
            self._log(f"Download concluído: {filename}, {self.expected_sequence_number} bytes")
        except Exception as e:
            self._log(f"ERRO: {e}")
            if file_started:
                self.file_manager.delete_file(filename)
        finally:
            self._clean_state()