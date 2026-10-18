"""A throwaway PKI for the mutual-TLS tests (spec §2.1, §4.4): a client CA, a server
certificate, client certificates of every shape the tests need, another CA, and CRLs.
Made with the `cryptography` library, never the openssl command line."""

import dataclasses
import datetime
import ipaddress
import ssl
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

NOW = datetime.datetime.now(tz=datetime.UTC)


def _key():
    return ec.generate_private_key(curve=ec.SECP256R1())


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(oid=NameOID.COMMON_NAME, value=common_name)])


@dataclasses.dataclass
class Issued:
    cert: x509.Certificate
    key: object
    cert_path: Path
    key_path: Path


class Authority:
    def __init__(self, directory: Path, common_name: str):
        self.directory = directory
        self.key = _key()
        self.cert = (x509.CertificateBuilder().subject_name(_name(common_name)).issuer_name(_name(common_name))
                     .public_key(self.key.public_key()).serial_number(x509.random_serial_number())
                     .not_valid_before(NOW - datetime.timedelta(minutes=5))
                     .not_valid_after(NOW + datetime.timedelta(days=2))
                     .add_extension(extval=x509.BasicConstraints(ca=True, path_length=None), critical=True)
                     .add_extension(extval=x509.KeyUsage(digital_signature=True, content_commitment=False,
                                                         key_encipherment=False, data_encipherment=False,
                                                         key_agreement=False, key_cert_sign=True, crl_sign=True,
                                                         encipher_only=False, decipher_only=False), critical=True)
                     .add_extension(extval=x509.SubjectKeyIdentifier.from_public_key(self.key.public_key()),
                                    critical=False)
                     .sign(private_key=self.key, algorithm=hashes.SHA256()))
        self.cert_path = directory / f"{common_name}.pem"
        self.cert_path.write_bytes(self.cert.public_bytes(encoding=serialization.Encoding.PEM))

    def issue(self, file_name: str, common_name: str, sans: list[x509.GeneralName] | None = None,
              server: bool = False) -> Issued:
        key = _key()
        builder = (x509.CertificateBuilder().subject_name(_name(common_name)).issuer_name(self.cert.subject)
                   .public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(NOW - datetime.timedelta(minutes=5))
                   .not_valid_after(NOW + datetime.timedelta(days=1))
                   .add_extension(extval=x509.ExtendedKeyUsage(
                       [ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]),
                       critical=False)
                   .add_extension(extval=x509.AuthorityKeyIdentifier.from_issuer_public_key(self.key.public_key()),
                                  critical=False))
        if sans:
            builder = builder.add_extension(extval=x509.SubjectAlternativeName(sans), critical=False)
        cert = builder.sign(private_key=self.key, algorithm=hashes.SHA256())
        cert_path, key_path = self.directory / f"{file_name}.pem", self.directory / f"{file_name}.key"
        cert_path.write_bytes(cert.public_bytes(encoding=serialization.Encoding.PEM))
        key_path.write_bytes(key.private_bytes(encoding=serialization.Encoding.PEM,
                                               format=serialization.PrivateFormat.PKCS8,
                                               encryption_algorithm=serialization.NoEncryption()))
        return Issued(cert=cert, key=key, cert_path=cert_path, key_path=key_path)

    def write_crl(self, path: Path, revoked: list[x509.Certificate]) -> None:
        builder = (x509.CertificateRevocationListBuilder().issuer_name(self.cert.subject)
                   .last_update(NOW - datetime.timedelta(minutes=5))
                   .next_update(NOW + datetime.timedelta(days=1)))
        for cert in revoked:
            builder = builder.add_revoked_certificate(
                x509.RevokedCertificateBuilder().serial_number(cert.serial_number)
                .revocation_date(NOW - datetime.timedelta(minutes=1)).build())
        crl = builder.sign(private_key=self.key, algorithm=hashes.SHA256())
        # Written whole and renamed into place, as an operator's tooling should.
        scratch = path.with_suffix(".tmp")
        scratch.write_bytes(crl.public_bytes(encoding=serialization.Encoding.PEM))
        scratch.replace(path)


class Pki:
    """Everything the mutual-TLS tests use, in `directory`."""

    def __init__(self, directory: Path):
        self.directory = directory
        self.ca = Authority(directory=directory, common_name="warp-test-client-ca")
        self.other_ca = Authority(directory=directory, common_name="warp-test-other-ca")
        self.server = self.ca.issue(file_name="server", common_name="localhost", server=True,
                                    sans=[x509.DNSName("localhost"),
                                          x509.IPAddress(ipaddress.ip_address("127.0.0.1"))])
        email = x509.RFC822Name
        self.alice = self.ca.issue(file_name="alice", common_name="alice", sans=[email("alice@example.com")])
        self.bob = self.ca.issue(file_name="bob", common_name="bob", sans=[email("bob@example.com")])
        # Another certificate (another key) for alice's identity: the same principal.
        self.alice_again = self.ca.issue(file_name="alice-again", common_name="alice",
                                         sans=[email("alice@example.com")])
        self.carol = self.ca.issue(file_name="carol", common_name="carol", sans=[email("carol@example.com")])
        self.dave = self.ca.issue(file_name="dave", common_name="dave", sans=[email("dave@example.com")])
        self.mallory = self.ca.issue(file_name="mallory", common_name="mallory",
                                     sans=[email("mallory@example.com")])
        self.two_emails = self.ca.issue(file_name="two-emails", common_name="two",
                                        sans=[email("alice@example.com"), email("bob@example.com")])
        self.cn_only = self.ca.issue(file_name="cn-only", common_name="alice@example.com")
        self.control = self.ca.issue(file_name="control", common_name="ctl",
                                     sans=[email("alice@example.com\x00.evil.example")])
        self.spiffe = self.ca.issue(file_name="spiffe", common_name="loader",
                                    sans=[x509.UniformResourceIdentifier("spiffe://example.org/ns/etl/sa/loader")])
        self.spiffe_other = self.ca.issue(file_name="spiffe-other", common_name="intruder",
                                          sans=[x509.UniformResourceIdentifier("spiffe://example.org/ns/x/sa/intruder")])
        self.alice_other_ca = self.other_ca.issue(file_name="alice-other-ca", common_name="alice",
                                                  sans=[email("alice@example.com")])
        self.crl_path = directory / "clients.crl.pem"
        self.ca.write_crl(path=self.crl_path, revoked=[self.carol.cert])

    def context(self, client: Issued | None) -> ssl.SSLContext:
        """A client TLS context trusting the test CA, presenting `client` (or nothing)."""
        context = ssl.create_default_context(cafile=str(self.ca.cert_path))
        if client is not None:
            context.load_cert_chain(certfile=str(client.cert_path), keyfile=str(client.key_path))
        return context
