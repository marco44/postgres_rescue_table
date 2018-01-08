This Perl script tries its best to extract whatever's left in a table after a corruption in a COPY format. Think ddrescue for PostgreSQL.

Most of the time, you are lucky, and there are only a few records corrupted, and you'll get most of your table back.

This script works on the assumption that your system tables are OK. As they are far less often updated than normal tables, that should be ok most of the time.

This script also requires that you managed to start your database somehow. You may need some pg_resetxlog to reset some corrupted system files (xlog, clog, etc...). MAKE A BACKUP BEFORE USING pg_resetxlogs. Or doing anything with rescue_tables anyway.


Please note the few following things:

* If you manage to extract a record, it doesn't mean it isn't corrupted. It only means it hasn't crashed PostgreSQL during its extraction and passed the controls in place in PostgreSQL's code. For simple data types, such as integers, these may easily still be corrupted
* If you have massive corruption, many records will end up full of nulls and zeros. You'll have to clean them up by yourself in the dumps. There will be a warning though
* If you have massive corruption, this script wont help you... data is lost, you'll spend too much time trying to find what's still usable
* This script may crash your database like crazy, meaning each time it will need to perform a recovery, which will be very slow. You can save a lot of time by using libeatmydata: https://www.flamingspork.com/projects/libeatmydata/. This will disable fsync, and will make recovery almost instantaneous. It wont make corruption worse, except if you suffer a system crash during your extraction (but then, you REALLY are unlucky :) ). Use it, it may save you hours. Once installed, start your PostgreSQL cluster with

  ```eatmydata pg_ctl -D my_instance_directory start```

* This script is as dumb as possible: 
  ```perl rescue_table.pl tablename filename.```

Everything else is passed by environment variables. See there: http://www.postgresql.org/docs/9.4/static/libpq-envars.html.

Or in a nutshell, set:

- PGDATABASE
- PGUSER
- PGPASSWORD

You may also need PGPORT, PGHOST...

I have initially been developping this as an internal Dalibo project (www.dalibo.org) to repair some customers' corrupted databases. Thank you.

This is distributed under GPL V3.
