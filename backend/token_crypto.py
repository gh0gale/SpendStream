"""
token_crypto.py — encrypts Gmail refresh tokens before they reach the database.

Fernet (AES-128-CBC with an HMAC-SHA256 check) keyed by TOKEN_ENCRYPTION_KEY.
Losing or changing the key makes every stored refresh token unreadable, and
those users have to reconnect Gmail.
"""

from cryptography.fernet import Fernet, InvalidToken

import config

__all__ = ["encrypt", "decrypt", "validate_key", "InvalidToken"]

_fernet: Fernet | None = None


def _cipher() -> Fernet:
    global _fernet
    if _fernet is None:
        config.require("TOKEN_ENCRYPTION_KEY")
        try:
            _fernet = Fernet(config.TOKEN_ENCRYPTION_KEY.encode())
        except ValueError as e:
            raise config.ConfigError(
                "TOKEN_ENCRYPTION_KEY is not a valid Fernet key. Generate one with: "
                'python -c "from cryptography.fernet import Fernet; '
                'print(Fernet.generate_key().decode())"'
            ) from e
    return _fernet


def validate_key() -> None:
    _cipher()


def encrypt(plaintext: str) -> str:
    return _cipher().encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    """Raises InvalidToken if the value was tampered with or used another key."""
    return _cipher().decrypt(ciphertext.encode()).decode()
