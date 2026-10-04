from pathlib import Path
from src.server.server import Server

HOST = "127.0.0.1"
PORT = 5000
PASTA = Path(__file__).resolve().parent / "files" / "server_files"


if __name__ == "__main__":
    PASTA.mkdir(parents=True, exist_ok=True)
    server = Server(base_dir=PASTA)
    server.bind((HOST, PORT))
    try:
        server.start_server()
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Encerrando.", flush=True)
    finally:
        for connection in list(server.connections.values()):
            if "timer" in connection:
                connection["timer"].cancel()
        if server.sock is not None:
            server.sock.close()
