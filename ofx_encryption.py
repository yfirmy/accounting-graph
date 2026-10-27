"""Encrypt imported OFX files with an explicitly configured short public-key ID."""
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile


def encryption_settings(config):
    if not config.getboolean('Encryption', 'enabled', fallback=False):
        return None
    key_id = config.get('Encryption', 'recipient_key_id', fallback='').strip()
    if not re.fullmatch(r'[0-9a-fA-F]{8}', key_id):
        raise ValueError('Encryption : recipient_key_id doit être un ID court (8 caractères hexadécimaux, sans préfixe 0x).')
    if shutil.which('gpg') is None:
        raise ValueError('Encryption : installer GnuPG (commande gpg introuvable).')
    return key_id.upper(), config.getboolean('Encryption', 'remove_plaintext', fallback=True)


def encrypt_ofx(filename, settings):
    """Publish ciphertext without overwriting anything; retain plaintext on failure."""
    key_id, remove_plaintext = settings
    source = Path(filename)
    original = source.lstat()
    if not stat.S_ISREG(original.st_mode):
        raise ValueError('Chiffrement OFX : un fichier ordinaire, sans lien symbolique, est requis.')
    destination = source.with_name(source.name + '.gpg')
    if os.path.lexists(destination):
        raise ValueError('Chiffrement OFX : le fichier .gpg existe déjà ; original conservé.')
    temporary = None
    try:
        with source.open('rb') as reader, tempfile.NamedTemporaryFile(
                dir=source.parent, prefix='.ofx-encrypted-', delete=False) as writer:
            temporary = Path(writer.name)
            # Ignore user gpg.conf so encrypt-to/default recipients cannot add recipients.
            # The configured short key ID is the explicit trust decision.
            result = subprocess.run([
                'gpg', '--no-options', '--batch', '--no-tty', '--trust-model', 'always',
                '--status-fd', '2', '--recipient', '0x' + key_id, '--output', '-', '--encrypt',
            ], stdin=reader, stdout=writer, stderr=subprocess.PIPE, check=False)
            writer.flush()
            os.fsync(writer.fileno())
            if (result.returncode or b'[GNUPG:] END_ENCRYPTION' not in result.stderr
                    or writer.tell() == 0):
                # GPG diagnostics may contain identities: do not echo them.
                raise ValueError('Échec du chiffrement GPG : vérifier la clé publique et sa capacité de chiffrement ; original conservé. L’import peut déjà être enregistré.')
        current = source.lstat()
        fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if any(getattr(current, field) != getattr(original, field) for field in fields):
            raise ValueError('Le fichier OFX a changé pendant le chiffrement ; original conservé.')
        # Atomic publication, with no replacement even if another process wins the race.
        os.link(temporary, destination)
        directory_fd = os.open(source.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if remove_plaintext:
            source.unlink()
        return destination
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
