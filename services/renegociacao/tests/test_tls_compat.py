import unittest
import warnings
from datetime import datetime, timedelta, timezone

from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from OpenSSL import crypto
from pytds.tls import validate_host


class SqlTlsCompatibility(unittest.TestCase):
    def test_driver_checks_certificate_hostname_with_supported_openssl_api(self):
        self.assertTrue(callable(getattr(crypto.X509, "get_extension", None)))
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "server.database.windows.net")])
        now = datetime.now(timezone.utc)
        certificate = (
            x509.CertificateBuilder()
            .subject_name(name).issuer_name(name)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(minutes=5))
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName("server.database.windows.net")]),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            peer = crypto.X509.from_cryptography(certificate)
            self.assertTrue(validate_host(peer, b"server.database.windows.net"))
            self.assertFalse(validate_host(peer, b"wrong.database.windows.net"))


if __name__ == "__main__":
    unittest.main()
