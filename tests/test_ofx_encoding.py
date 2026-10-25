import tempfile
from pathlib import Path
import unittest

from ofx_input import read_ofx


OFX = '''OFXHEADER:100
DATA:OFXSGML
VERSION:102
SECURITY:NONE
ENCODING:{encoding}
CHARSET:1252
COMPRESSION:NONE
OLDFILEUID:NONE
NEWFILEUID:NONE
<OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS><CURDEF>EUR
<BANKACCTFROM><BANKID>1<ACCTID>123<ACCTTYPE>CHECKING</BANKACCTFROM>
<BANKTRANLIST><DTSTART>20261001<DTEND>20261025
<STMTTRN><TRNTYPE>DEBIT<DTPOSTED>20261024<TRNAMT>-10<FITID>abc<MEMO>{label}</STMTTRN>
</BANKTRANLIST><LEDGERBAL><BALAMT>100<DTASOF>20261025</LEDGERBAL>
</STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>'''


class EncodingTests(unittest.TestCase):
    def test_decoding_and_original_preserved(self):
        for charset, declaration, bom, label in [
                ('utf-8', 'USASCII', b'', 'Prélèvement € – œ'),
                ('utf-8', 'UTF-8', b'', 'Prélèvement'),
                ('utf-8', 'USASCII', b'\xef\xbb\xbf', 'Prélèvement'),
                ('cp1252', 'USASCII', b'', 'Prélèvement € – œ'),
                ('ascii', 'USASCII', b'', 'PRELEVEMENT')]:
            with self.subTest(charset=charset, declaration=declaration, bom=bom):
                data = bom + OFX.format(encoding=declaration, label=label).encode(charset)
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / 'test.ofx'
                    path.write_bytes(data)
                    parsed = read_ofx(path)
                    self.assertEqual(parsed.account.statement.transactions[0].memo, label)
                    self.assertEqual(path.read_bytes(), data)

    def test_invalid_declared_utf8_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'invalid.ofx'
            path.write_bytes(OFX.format(encoding='UTF-8', label='Prélèvement').encode('cp1252'))
            with self.assertRaises(UnicodeDecodeError):
                read_ofx(path)


if __name__ == '__main__':
    unittest.main()
