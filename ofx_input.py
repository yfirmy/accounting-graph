"""Locate and archive the newest downloaded OFX without replacing existing files."""
import datetime
import io
import os
from pathlib import Path
import re
import shutil

from ofxparse import OfxParser


def read_ofx(path):
    """Parse original bytes, correcting UTF-8 mislabeled as a legacy charset.

    Some bank exports declare USASCII/1252 despite containing UTF-8. Only
    override that declaration when the entire file is strictly valid UTF-8
    and contains non-ASCII bytes. Keep the archived file unchanged.
    """
    data = Path(path).read_bytes()
    try:
        data.decode('utf-8-sig', errors='strict')
    except UnicodeDecodeError:
        pass  # Let ofxparse decode the declared legacy charset strictly.
    else:
        header, separator, body = data.partition(b'<')
        if separator and any(byte >= 128 for byte in data):
            if header.startswith(b'\xef\xbb\xbf'):
                header = header[3:]
            if re.search(br'(?mi)^ENCODING:[^\r\n]*', header):
                header = re.sub(br'(?mi)^ENCODING:[^\r\n]*', b'ENCODING:UTF-8', header)
                data = header + separator + body
    with io.BytesIO(data) as stream:
        return OfxParser.parse(stream)


def print_file_dates(path):
    path = Path(path)
    info = path.stat()
    print('\033[94mFichier OFX : {}\033[0m'.format(path))
    def formatted(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).astimezone().strftime('%d/%m/%Y %H:%M:%S %z')
    print('Dernière modification : ' + formatted(info.st_mtime))
    if hasattr(info, 'st_birthtime'):
        print('Création : ' + formatted(info.st_birthtime))
    age = datetime.datetime.now().timestamp() - info.st_mtime
    if age >= 0:
        print('Ancienneté depuis modification : {} jour(s), {} heure(s)'.format(
            int(age // 86400), int(age % 86400 // 3600)))
    else:
        print('\033[33mAttention : la date de modification est dans le futur.\033[0m')


def _move_without_overwrite(source, destination):
    """Copy exclusively, preserve mtime, then remove the downloaded original.

    Works across volumes. A failed/incomplete copy is removed and the source is
    retained. An existing destination is never overwritten, even in a race.
    """
    original = source.stat()
    created = False
    try:
        with source.open('rb') as reader, destination.open('xb') as writer:
            created = True
            shutil.copyfileobj(reader, writer)
            writer.flush()
            os.fsync(writer.fileno())
        current = source.stat()
        if ((current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
                != (original.st_dev, original.st_ino, original.st_size, original.st_mtime_ns)):
            raise ValueError('Le fichier téléchargé a changé pendant la copie ; déplacement annulé.')
        os.utime(destination, ns=(original.st_atime_ns, original.st_mtime_ns))
        source.unlink()
    except Exception:
        if created:
            destination.unlink()
        raise


def collect_latest_ofx(config, dry_run=False):
    def required(section, key):
        value = config.get(section, key, fallback='').strip()
        if not value:
            raise ValueError('Renseigner [{}] {} dans properties.ini pour l’import automatique.'.format(section, key))
        return value
    downloads = Path(required('Directories', 'downloads')).expanduser()
    archive = Path(required('Directories', 'input')).expanduser()
    pattern = re.compile(required('Files', 'ofx_filename_regex'))
    if downloads.resolve() == archive.resolve():
        raise ValueError('Les répertoires downloads et input doivent être différents.')
    if not downloads.is_dir():
        raise ValueError('Répertoire de téléchargements introuvable : ' + str(downloads))
    candidates = [(path.stat().st_mtime_ns, path.name, path) for path in downloads.iterdir()
                  if not path.is_symlink() and path.is_file() and pattern.fullmatch(path.name)]
    if not candidates:
        print('Aucun fichier OFX correspondant à la regex dans ' + str(downloads))
        return None
    source = max(candidates)[2]
    print_file_dates(source)
    destination = archive / source.name
    # lexists also protects a dangling symlink or a directory with this name.
    if os.path.lexists(destination) or os.path.lexists(str(destination) + '.gpg'):
        print('Arrêt : le fichier était déjà intégré dans le répertoire de stockage : ' + str(destination))
        return None
    if dry_run:
        print('Simulation : déplacement prévu vers {} (aucun fichier déplacé).'.format(destination))
        return source
    archive.mkdir(parents=True, exist_ok=True)
    try:
        _move_without_overwrite(source, destination)
    except FileExistsError:
        print('Arrêt : le fichier était déjà intégré dans le répertoire de stockage : ' + str(destination))
        return None
    print('Fichier déplacé vers : ' + str(destination))
    return destination
