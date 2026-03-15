"""EarthGrid Node Identity — Ed25519 keypair for P2P authentication.

Each node generates a unique Ed25519 keypair on first start.
The public key serves as the node's identity across the network.
Requests are signed with the private key — peers verify with the public key.
No secrets to share or leak.
"""
from __future__ import annotations
import base64
import json
import logging
import time
from pathlib import Path

from nacl.signing import SigningKey, VerifyKey
from nacl.exceptions import BadSignatureError

logger = logging.getLogger("earthgrid.identity")


class NodeIdentity:
    """Ed25519 keypair for node authentication."""

    def __init__(self, key_path: Path):
        """Load or generate keypair.

        Args:
            key_path: Path to store the private key (e.g., /data/.node_key)
        """
        self.key_path = key_path
        key_path.parent.mkdir(parents=True, exist_ok=True)

        if key_path.exists():
            raw = base64.b64decode(key_path.read_text().strip())
            self._signing_key = SigningKey(raw)
            self._verify_key = self._signing_key.verify_key
            logger.info(f"Loaded node identity: {self.public_key_b64[:16]}...")
        else:
            self._signing_key = SigningKey.generate()
            self._verify_key = self._signing_key.verify_key
            key_path.write_text(base64.b64encode(bytes(self._signing_key)).decode())
            key_path.chmod(0o600)
            logger.warning(f"Generated new node identity: {self.public_key_b64[:16]}...")

    @property
    def public_key_b64(self) -> str:
        """Base64-encoded public key (shareable, used as node identity)."""
        return base64.b64encode(bytes(self._verify_key)).decode()

    @property
    def public_key_hex(self) -> str:
        """Hex-encoded public key."""
        return bytes(self._verify_key).hex()

    def sign(self, message: str) -> str:
        """Sign a message, return base64-encoded signature."""
        signed = self._signing_key.sign(message.encode())
        return base64.b64encode(signed.signature).decode()

    def sign_exchange(self, node_name: str, node_id: str, api_key: str,
                      timestamp: int | None = None) -> dict:
        """Create a signed key exchange payload.

        Returns dict with: node_name, node_id, api_key, public_key, timestamp, signature
        The signature covers: "{node_name}|{node_id}|{api_key}|{timestamp}"
        """
        ts = timestamp or int(time.time())
        msg = f"{node_name}|{node_id}|{api_key}|{ts}"
        sig = self.sign(msg)
        return {
            "node_name": node_name,
            "node_id": node_id,
            "api_key": api_key,
            "public_key": self.public_key_b64,
            "timestamp": ts,
            "signature": sig,
        }

    @staticmethod
    def verify_exchange(payload: dict, max_age_seconds: int = 300) -> bool:
        """Verify a signed key exchange payload from a peer.

        Args:
            payload: dict with node_name, node_id, api_key, public_key, timestamp, signature
            max_age_seconds: reject payloads older than this (replay protection)

        Returns True if valid, False otherwise.
        """
        try:
            pub_key_bytes = base64.b64decode(payload["public_key"])
            verify_key = VerifyKey(pub_key_bytes)

            ts = payload["timestamp"]
            # Replay protection
            age = abs(int(time.time()) - ts)
            if age > max_age_seconds:
                logger.warning(f"Key exchange replay: age={age}s > max={max_age_seconds}s")
                return False

            msg = f"{payload['node_name']}|{payload['node_id']}|{payload['api_key']}|{ts}"
            sig = base64.b64decode(payload["signature"])
            verify_key.verify(msg.encode(), sig)
            return True
        except (BadSignatureError, KeyError, Exception) as e:
            logger.warning(f"Key exchange verification failed: {e}")
            return False

    @staticmethod
    def verify_request(public_key_b64: str, signature_b64: str,
                       message: str) -> bool:
        """Verify an arbitrary signed message from a known peer."""
        try:
            pub_key_bytes = base64.b64decode(public_key_b64)
            verify_key = VerifyKey(pub_key_bytes)
            sig = base64.b64decode(signature_b64)
            verify_key.verify(message.encode(), sig)
            return True
        except (BadSignatureError, Exception):
            return False
