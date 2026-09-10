import asyncio
import hashlib
import hmac
import ipaddress
import socket
import time
from urllib.parse import urlsplit

from aiohttp.abc import AbstractResolver
from cryptography.fernet import Fernet

from .config import settings


def cipher():
    return Fernet(settings.encryption_key.encode())


def public_unicast(address) -> bool:
    return address.is_global and not (
        address.is_multicast
        or address.is_loopback
        or address.is_link_local
        or address.is_unspecified
        or address.is_reserved
    )


def validate_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        hostname = (parts.hostname or "").lower()
        port = parts.port
    except ValueError as error:
        raise ValueError("Invalid endpoint URL") from error
    exception = hostname in settings.allowed_insecure_hosts
    if not hostname or parts.username or parts.password or parts.fragment:
        raise ValueError("URL must have a hostname and no credentials or fragment")
    if parts.scheme != "https" and not (exception and parts.scheme == "http"):
        raise ValueError("HTTPS is required")
    if port is not None and port != 443 and not exception:
        raise ValueError("Only standard HTTPS ports are supported")
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address is not None and not public_unicast(address) and not exception:
        raise ValueError("Private and reserved destinations are forbidden")
    return hostname


class PublicResolver(AbstractResolver):
    """Validate every DNS answer and return pinned IPs directly to TCPConnector."""

    async def resolve(self, host, port=0, family=socket.AF_INET):
        records = await asyncio.get_running_loop().getaddrinfo(
            host, port, type=socket.SOCK_STREAM, family=family
        )
        results = []
        for address_family, _, proto, _, sockaddr in records:
            ip = sockaddr[0]
            if (
                not public_unicast(ipaddress.ip_address(ip))
                and host.lower() not in settings.allowed_insecure_hosts
            ):
                raise OSError("Endpoint resolves to a private or reserved address")
            results.append(
                {
                    "hostname": host,
                    "host": ip,
                    "port": port,
                    "family": address_family,
                    "proto": proto,
                    "flags": socket.AI_NUMERICHOST,
                }
            )
        if not results:
            raise OSError("Endpoint DNS returned no addresses")
        return results

    async def close(self):
        return None


def signature(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return "v1=" + digest


def verify_signature(
    secret: str, timestamp: str, body: bytes, supplied: str, tolerance=300
) -> bool:
    try:
        if abs(time.time() - int(timestamp)) > tolerance:
            return False
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(signature(secret, timestamp, body), supplied)
