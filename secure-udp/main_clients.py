from ipaddress import IPv4Address
from os import getpid
from pathlib import Path

from src.client.client import Client

PASTA = Path(__file__).resolve().parent / "files" / "client_files"


def configurar_servidor(atual):
    while True:
        try:
            ip = input(f"IP do servidor [{atual[0]}]: ").strip() or atual[0]
            ip = str(IPv4Address(ip))
            porta = int(input(f"Porta [{atual[1]}]: ").strip() or atual[1])
            if not 1 <= porta <= 65535:
                raise ValueError
            return ip, porta
        except ValueError:
            print("Informe um IPv4 válido e uma porta entre 1 e 65535.")


def escolher_blocos():
    while True:
        entrada = input("Blocos a descartar, separados por vírgula [3,7]: ").strip() or "3,7"
        try:
            blocos = {int(valor.strip()) for valor in entrada.split(",")}
            if any(bloco < 1 for bloco in blocos):
                raise ValueError
            return blocos
        except ValueError:
            print("Use números positivos, por exemplo: 3,7. O primeiro bloco é 1.")


def main():
    nome_cliente = f"client_{getpid()}"
    destino = PASTA / nome_cliente
    destino.mkdir(parents=True, exist_ok=True)
    servidor = configurar_servidor(("127.0.0.1", 5000))

    while True:
        print(f"\n[{nome_cliente}] Servidor: {servidor[0]}:{servidor[1]}")
        print(f"Destino: {destino}")
        print("1 - Solicitar arquivo sem perdas no cliente")
        print("2 - Solicitar arquivo descartando blocos")
        print("3 - Alterar IP/porta do servidor")
        print("0 - Sair")
        opcao = input("Opção: ").strip()
        if opcao == "0":
            return
        if opcao == "3":
            servidor = configurar_servidor(servidor)
            continue
        if opcao not in {"1", "2"}:
            print("Opção inválida.")
            continue

        filename = input("Arquivo: ").strip()
        blocos = escolher_blocos() if opcao == "2" else set()
        print(f"Solicitando {filename}; blocos descartados: {sorted(blocos)}", flush=True)
        client = Client(server_addr=servidor, base_path=destino, discard_blocks=blocos)
        try:
            client.recvfrom(filename)
        except (ValueError, OSError) as error:
            print(f"ERRO: {error}")
        finally:
            client.timer.cancel()
            client.sock.close()


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nCliente encerrado.")
