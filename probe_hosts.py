import asyncio
import socket

async def check_host(host):
    try:
        print(f"Checking {host}...")
        addr = socket.gethostbyname(host)
        print(f"Found IP: {addr}")
    except Exception as e:
        print(f"Host not found or error: {e}")

if __name__ == "__main__":
    hosts = [
        "ep-polished-union-55158691.eu-central-1.aws.neon.tech",
        "ep-polished-union-55158691-pooler.eu-central-1.aws.neon.tech",
        "ep-weathered-recipe-agaj3m4t.eu-central-1.aws.neon.tech",
        "ep-weathered-recipe-agaj3m4t-pooler.eu-central-1.aws.neon.tech"
    ]
    for h in hosts:
        asyncio.run(check_host(h))
