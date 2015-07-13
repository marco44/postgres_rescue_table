#!/usr/bin/perl -w
use DBI;
use strict;
use FileHandle;
use Time::HiRes;

my $dbh;
my $table=$ARGV[0];
my $outfile=$ARGV[1];

my %fails; # Will contain per block and per record failures


my $curr_page=0;
my $curr_rec=1;
my $blocksize;
my $tableblocks;
my $max_records_per_block;
my $batch_ctids_max=1000000;
my $batch_ctids_min=100;

# FIXME: detect allnulls ?
sub reconnect_if_dead
{
        my $ok;
        my $first_try=1;
        eval {$ok=$dbh->do ("SELECT 1")};
	while (not $ok)
	{
                $dbh->disconnect() if (defined $dbh);
                if ($first_try)
                {
                    $first_try=0;
                }
                else
                {
                    Time::HiRes::usleep 1000; # Sleep 1ms
                }
		eval {$dbh = DBI->connect("dbi:Pg:",undef,undef,{RaiseError => 1,PrintError => 0})};
                if ($dbh)
                {
		    eval {$ok=$dbh->do ("SET lc_messages TO 'C'")}; # need english messages to get page headers errors
                }
                else
                {
                    $ok=0;
                }
	}
}

sub max ($$) { $_[$_[0] < $_[1]] };
sub min ($$) { $_[$_[0] > $_[1]] };

sub next_ctid
{
    my ($ctid)=@_;
    $ctid=~ '^\((-?\d+),(\d+)\)$' or die "cannot understand ctid : $ctid";
    my ($block,$id)=($1,$2);
    $id++;
    if ($id>$max_records_per_block)
    {
        $id=0;
        $block++;
    }
    return "($block,$id)";
}

sub last_ctid_page
{
    my ($page)=@_;
    my $ctid='(' . $page . ',' . $max_records_per_block . ')';
    return $ctid;
}

sub page_from_ctid
{
    my ($ctid)=@_;
    $ctid=~ '^\((\d+),(\d+)\)$' or die "cannot understand ctid : $ctid";
    return($1);
}

# Checks if we get a page header error. If it is, no point in trying to go further in this page
sub is_page_corrupted
{
    my ($message)=@_;
    return 1 if ($message =~ /invalid page header in block/);
    return 0;
}

sub extract_one_by_one
{
    my ($last_ok_ctid)=@_;
    my $start_ctid=next_ctid($last_ok_ctid);
    my $current_page=page_from_ctid($start_ctid);
    my $current_ctid=$start_ctid;
    # Usually corruption occurs per block. finish current block
    while(page_from_ctid($current_ctid) == $current_page)
    {
        reconnect_if_dead();
        my $query="COPY (SELECT ctid,* from $table where ctid ='$current_ctid') TO STDOUT";
        eval{$dbh->do($query)}
        or do
        {
                print STDERR "Cannot start COPY for $current_ctid\n";
                $fails{ctid}->{$current_ctid}=1;
                $current_ctid=next_ctid($current_ctid);
                $dbh->disconnect() if (defined $dbh);
                next;
        };
        my $status;
        my $line;
        eval {$status=$dbh->pg_getcopydata($line)}
        or do
        {
                print STDERR $current_ctid, " corrupt, cannot extract\n";
                if (is_page_corrupted($dbh->errstr))
                {
                    print STDERR "page " . page_from_ctid($current_ctid) . " corrupt. Skipping\n";
                    # Tell the caller we reached the end of the page
                    $fails{page}->{page_from_ctid($current_ctid)}=1;
                    return last_ctid_page(page_from_ctid($current_ctid));
                }
                else
                {
                    $current_ctid=next_ctid($current_ctid);
                    $fails{ctid}->{$current_ctid}=1;
                    $dbh->disconnect() if (defined $dbh);
                }
                next;
        };
        if ($status==-1) # No record for this ctid
        {
                $current_ctid=next_ctid($current_ctid);
                next;
        }
        $line=~/^\((\d+),(\d+)\)\t(.*)/ or die "cannot understand $line\n";
        my $data=$3;
        if ($line=~/^(\\N\t|0\t)*\\N$/)
        {
            # Line only nulls. Corrupted, happens if zeroed
            print STDERR $current_ctid," probably corrupt, only nulls\n";
            $fails{ctid}->{$current_ctid}=0; # Not really a failed record, but high probability
            next;
        }

        print OUTFILE $data,"\n";
        print $current_ctid,": OK\n";
        $current_ctid=next_ctid($current_ctid);
    }
    return $current_ctid;
}

sub data_dump
{
        my $last_ok_ctid="(-1,$max_records_per_block)";
        my $batch_size=$batch_ctids_min; # Initial size of batch;
        MAIN:
        while (1)
        {
	    my $where_cond='';
 	    my @ctids;
            my $batch_ctid=$last_ok_ctid;
            my $start_ctid=next_ctid($last_ok_ctid);
            $batch_size=min($batch_ctids_max,$batch_size*10);
            return if (page_from_ctid($start_ctid) >= $tableblocks); # Block number starts at 0
            for (my $count=0;$count<$batch_size;$count++)
            {
                $batch_ctid=next_ctid($batch_ctid);
                push @ctids,("'" . $batch_ctid . "'");
            }
            $where_cond=join(',',@ctids);
            reconnect_if_dead();
            print STDERR "page " . page_from_ctid($start_ctid) . " to " . page_from_ctid($batch_ctid) . " out of $tableblocks\n";
            my $query="COPY (SELECT ctid,* from $table where ctid IN ($where_cond)) TO STDOUT";
            my $status;
            eval { $status=$dbh->do($query)}
              or do 
              {
                  print STDERR "something corrupt on page " . page_from_ctid(next_ctid($last_ok_ctid)) . " or next block\n";
                  print STDERR "extracting next few records one by one\n";
                  $dbh->disconnect() if (defined $dbh);
                  $last_ok_ctid=extract_one_by_one($last_ok_ctid);
                  # We extracted a few records one by one. Time to go back to batch
                  $batch_size=$batch_ctids_min;
                  next MAIN;
               };
            my $lines_extracted=0;
            while (1)
            {	
	        my $line;
	        my $status;
                eval {$status=$dbh->pg_getcopydata($line)}
                    or do
                    {
                            print STDERR "something corrupt on page " . page_from_ctid(next_ctid($last_ok_ctid)) . " or next block\n";
                            print STDERR "extracting next few records one by one\n";
                            $last_ok_ctid=extract_one_by_one($last_ok_ctid);
                            # We extracted a few records one by one. Time to go back to batch
                            $batch_size=$batch_ctids_min;
                            next MAIN;
                    };
                    if ($status < 0)
                    {
                        # We ended this batch. Even if we didn't see this ctid
                        # because it may not exist, we will start from there on next pass
                        $last_ok_ctid=$batch_ctid;
                        next MAIN;
                    }
                    $line=~/^\((\d+),(\d+)\)\t(.*)/ or die "cannot understand $line\n";
                    $last_ok_ctid="($1,$2)";
                    print OUTFILE $3, "\n";
            }
        }
}

sub cmp_ctid
{
    my ($a,$b)=@_;
    my ($pagea,$ida)= ($a =~ /^\((\d+),(\d+)\)$/);
    my ($pageb,$idb)= ($b =~ /^\((\d+),(\d+)\)$/);
    return ($pagea<=>$pageb) if ($pagea<=>$pageb);
    return ($ida<=>$idb);
}

sub print_report
{
    # Nothing done, nothing to print
    return if ($tableblocks==0);

    # First, let's do a little bit of cleanup
    # Some individual records may have been identified as failed while the whole block is failed. Remove them
    my ($key,$value);
    while (($key,$value)=each(%{$fails{ctid}})){
        if (defined($fails{page}->{page_from_ctid($key)}))
        {
            delete $fails{ctid}->{$key};
        }
    }
    # Is there corruption ?
    return unless ((defined $fails{ctid} and scalar(%{$fails{ctid}})) 
                       or 
                    (defined $fails{page} and scalar(%{$fails{page}})));

    # Now print the summary
    print STDOUT "=========================================\n";
    print STDOUT "Corrupted pages:\n";
    print join(',', sort{$a<=>$b}(keys(%{$fails{page}})));
    print STDOUT "=========================================\n";
    print STDOUT "Corrupted records:\n";
    print join(',', sort{cmp_ctid($a,$b)}(keys(%{$fails{ctid}})));

}




# No authentification and all. Use env variables

reconnect_if_dead();

# Get blocksize
my $block_rec=$dbh->selectall_arrayref("SELECT setting from pg_settings where name='block_size'") or die "block_size not in pg_settings\n";
my $blocksize_bytes=$block_rec->[0]->[0];
$blocksize=$blocksize_bytes/1024;
$max_records_per_block=32*$blocksize;
# Get table size
my $table_size_rec=$dbh->selectall_arrayref("SELECT pg_relation_size('$table')");
$tableblocks=$table_size_rec->[0]->[0]/$blocksize_bytes;


open OUTFILE,">$outfile" or die "Cannot open $outfile for write, $!\n";
#OUTFILE->autoflush(1); # FIXME: maybe remove ? very useful for debugging, when extraction is slow

# Here is how it works: copy ctid,* from table, send what we can to a file.
# Memorize latest ctid. If fails, resume with ctid+1 (1024 max on a 32kb page). Reconnect if fails

data_dump();

# Generate a report
print_report();



print "\n"; # Just so that the user knows we're through :)
