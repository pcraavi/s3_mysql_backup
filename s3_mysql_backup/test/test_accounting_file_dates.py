from datetime import datetime as dt
from s3_mysql_backup.backup_file import gnu_file_date
from s3_mysql_backup.backup_file import qb_file_date


class Test:

    def tests_file_dates(self):
        gffile = 'Personal041008.20140819135748.gnucash.20151005135235.gnucash.20160921104022.gnucash'
        qfile = 'ROCKETS_REDGLARE_2005 (Backup Sep 22,2016  09 41 AM).QBB'
        assert gnu_file_date(gffile) == dt(2016, 9, 21, 10, 40, 22)
        assert qb_file_date(qfile) == dt(year=2016, month=9, day=22, hour=9, minute=41)
