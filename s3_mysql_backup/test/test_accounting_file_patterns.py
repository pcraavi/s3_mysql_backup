import re
from s3_mysql_backup.scripts.backup_gnucash import pat as gpat
from s3_mysql_backup.scripts.backup_qb import pat as qpat


class Test:
    assert re.match(gpat, 'Personal041008.20140819135748.gnucash.20151005135235.gnucash.20160921104022.gnucash')
    assert re.match(gpat, 'Personal041008.20140819135748.gnucash.20151005135235.gnucash.20160923091326.gnucash')
    assert re.match(gpat, 'Personal041008.20140819135748.gnucash.20151005135235.gnucash.20160923092006.gnucash')
