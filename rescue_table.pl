#!/usr/bin/perl -w
use DBI;
use strict;

my $dbh;
my $table=$ARGV[0];
my $outfile=$ARGV[1];


sub reconnect_if_dead
{
	my $ok=$dbh->do ("SELECT 1");
	unless ($ok)
	{
		$dbh = DBI->connect("dbi:Pg",undef,undef,{RaiseError => 1,PrintError => 0});
		$ok=$dbh->do ("SELECT 1");
	}
}

my $curr_page=0;
my $curr_rec=1;
my $blocksize;
my $tableblocks;
my $max_records_per_block;

sub dump_block
{
	my $where_cond='';
	my @ctids;
	my $data='';
	my $status;
	my $line;
	my $one_by_one=0;
	for (my $rec=1;$rec<=$max_records_per_block;$rec++)
	{
		push @ctids,("'($curr_page,$rec)'");
	}
	$where_cond=join(',',@ctids);
	reconnect_if_dead();
	print STDOUT "block $curr_page out of $tableblocks\n";
	my $query="COPY (SELECT ctid,* from $table where ctid IN ($where_cond)) TO STDOUT";
	$dbh->do($query);
        while (1)
	{	eval {$status=$dbh->pg_getcopydata($line)}
	        or do
                {
               	        print STDERR "something corrupt on page $curr_page\n";
			$one_by_one=1;
			last;
	        };
		last if ($status < 0);
	        $line=~/^\((\d+),(\d+)\)\t(.*)/ or die "cannot understand $line\n";
		$data.=$3;
	}
	unless ($one_by_one) # We managed to extract the whole bloc in one go
	{
		return $data;
	}
	# Ok, some record was bad. We have to extract them one by one in this block
	$data='';
	for (my $rec=1;$rec<=$max_records_per_block;$rec++)
	{
		reconnect_if_dead();
		my $query="COPY (SELECT ctid,* from $table where ctid ='($curr_page,$rec)') TO STDOUT";
		$dbh->do($query);
	        eval {$status=$dbh->pg_getcopydata($line)}
	        or do
       	        {
               	        print STDERR "record ($curr_page,$rec) corrupt\n";
                        next;
                };
	        if ($status==-1) # No record for this ctid
	        {
        	        next;
	        }
        	$line=~/^\((\d+),(\d+)\)\t(.*)/ or die "cannot understand $line\n";
	
	        $data.=$3;
	}
	return $data
}



# No authentification and all. Use env variables

$dbh = DBI->connect("dbi:Pg:",undef,undef,{RaiseError => 1,PrintError => 0}) or die "Cannot connect\n";

# Get blocksize
my $block_rec=$dbh->selectall_arrayref("SELECT setting from pg_settings where name='block_size'") or die "block_size not in pg_settings\n";
my $blocksize_bytes=$block_rec->[0]->[0];
$blocksize=$blocksize_bytes/1024;
$max_records_per_block=32*$blocksize;
# Get table size
my $table_size_rec=$dbh->selectall_arrayref("SELECT pg_relation_size('$table')");
$tableblocks=$table_size_rec->[0]->[0]/$blocksize;


open OUTFILE,">$outfile" or die "Cannot open $outfile for write, $!\n";

# Here is how it works: copy ctid,* from table, send what we can to a file.
# Memorize latest ctid. If fails, resume with ctid+1 (1024 max on a 32kb page). Reconnect if fails

my $eot=0;
while (not $eot)
{
	print OUTFILE dump_block();
	$curr_page++;
	if ($curr_page > $tableblocks)
	{
		$eot=1;
	}
}
