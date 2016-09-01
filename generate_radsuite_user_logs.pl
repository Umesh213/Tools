#!/usr/bin/perl -w

use strict;
use DBI qw( neat_list );
use Getopt::Long;
use vars qw($counter
                        $pidfile
                        $noconfirm
                        $logpath
                        $logpath
                        $logfile_prefix
                        $logfile_incr
                        $ldapuser_sth
                        $localuser_sth
                        $qc_act_sth
                        $log
                        $failedlog
                        $checklogfilesize
                        $checkfailedlogfilesize
                        $firstlog
                        $firstfailedlog
						$getoptresults
						$help
                        );

# Set the signal interrups
$SIG{INT} = \&Scram;     # CTRL-C, kill -2
$SIG{QUIT} = \&Scram;    # CTRL-D, kill -3
$SIG{TERM} = \&Scram;    # kill or kill -15
system("clear");

local $| = 1;
my ($dbh, @row, $ctr);
$ctr = 0;
my $ver = 1;
my $logcounter = 0;
my $failedlogcounter = 0;

eval 
	{
	$getoptresults = GetOptions ( 
			'help|h' 			=> \$help,
			'no-confirm|n'		=> \$noconfirm,
			'path|p=s'			=> \$logpath);
	};
if($@)
	{
	print "Incorrect usage: $@\n\n";
	parameter_printout();
	exit;
	}
if(!$getoptresults || $help || !$logpath)
	{
	parameter_printout();
	exit;		
	}

check_for_existing_process();
get_activity();

sub parameter_printout
	{
	print "You are required to add a destination file path for the logs being generated.\n\n";
	print "Options:\n";
	print "\t-p or --path                The destination directory of log files to be generated\n";
	print "\t-n or --no-confirm          Allow script to run to completion without prompting\n";
	print "\n";
	print "Example: $0 --path <path> -n\n\n";
	}

sub get_activity
        {
        print "Log files are limited to 1GB in size for each file.\n";
        if (! $noconfirm)
        	{
        	print "Press Any Key to continue or Ctrl^C to exit.\n";
        	<STDIN>;
        	}
        Open_Database('AS_USER', 'AS_USER');
        my $av_sth_count = $dbh->prepare("SELECT COUNT(1)
        FROM AS_USER.USER_SESSION
        LEFT JOIN AS_USER.APP_USER ON AS_USER.APP_USER.USER_SEQ=AS_USER.USER_SESSION.USER_SEQ
        LEFT JOIN AS_USER.APP_USER_GROUP ON AS_USER.APP_USER.USER_SEQ = AS_USER.APP_USER_GROUP.USER_SEQ
        LEFT JOIN AS_USER.APP_GROUP ON AS_USER.APP_GROUP.GROUP_SEQ = AS_USER.APP_USER_GROUP.GROUP_SEQ
        LEFT JOIN AS_USER.APP_USER_LDAP_SOURCE ON AS_USER.APP_USER_LDAP_SOURCE.USER_SEQ = AS_USER.APP_USER.USER_SEQ
        LEFT JOIN AS_USER.APP_GROUP_LDAP_SOURCE ON AS_USER.APP_GROUP_LDAP_SOURCE.GROUP_SEQ = AS_USER.APP_USER_GROUP.GROUP_SEQ
        LEFT JOIN AS_USER.LDAP_SOURCE ON AS_USER.LDAP_SOURCE.LDAP_SOURCE_SEQ=AS_USER.APP_USER_LDAP_SOURCE.LDAP_SOURCE_SEQ") 
        or die "Can't prepare 'av_sth': " . $DBI::errstr . "\n";
        # Connect to the db
        $av_sth_count->execute();
        my $entry_count = $av_sth_count->fetchrow_array();
        
        my $av_sth = $dbh->prepare("SELECT AS_USER.APP_USER.UPPER_USER_ID
        , AS_USER.APP_USER.FAMILYNAME
        , AS_USER.APP_USER.GIVENNAME
        , AS_USER.APP_USER.MIDDLENAME
        , AS_USER.APP_USER.ORGANIZATION
        , AS_USER.APP_USER.EMAIL_ADDRESS
        , TO_CHAR(AS_USER.USER_SESSION.START_TIME, 'YYYYMMDD HH24:MI:SS')
        , TO_CHAR(AS_USER.USER_SESSION.END_TIME, 'YYYYMMDD HH24:MI:SS')
        , TO_CHAR(AS_USER.USER_SESSION.LAST_ACCESSED_TIME, 'YYYYMMDD HH24:MI:SS')
        , AS_USER.USER_SESSION.CLIENT_IP_ADDRESS
        , AS_USER.APP_GROUP.GROUP_NAME
        , AS_USER.APP_USER_LDAP_SOURCE.DESCRIPTION
        , AS_USER.APP_GROUP_LDAP_SOURCE.DESCRIPTION
        , AS_USER.LDAP_SOURCE.LDAP_SOURCE_NAME
        , AS_USER.LDAP_SOURCE.DOMAIN
        , AS_USER.LDAP_SOURCE.DOMAIN_SUFFIX
        , AS_USER.LDAP_SOURCE.DESCRIPTION
        FROM AS_USER.USER_SESSION
        LEFT JOIN AS_USER.APP_USER ON AS_USER.APP_USER.USER_SEQ=AS_USER.USER_SESSION.USER_SEQ
        LEFT JOIN AS_USER.APP_USER_GROUP ON AS_USER.APP_USER.USER_SEQ = AS_USER.APP_USER_GROUP.USER_SEQ
        LEFT JOIN AS_USER.APP_GROUP ON AS_USER.APP_GROUP.GROUP_SEQ = AS_USER.APP_USER_GROUP.GROUP_SEQ
        LEFT JOIN AS_USER.APP_USER_LDAP_SOURCE ON AS_USER.APP_USER_LDAP_SOURCE.USER_SEQ = AS_USER.APP_USER.USER_SEQ
        LEFT JOIN AS_USER.APP_GROUP_LDAP_SOURCE ON AS_USER.APP_GROUP_LDAP_SOURCE.GROUP_SEQ = AS_USER.APP_USER_GROUP.GROUP_SEQ
        LEFT JOIN AS_USER.LDAP_SOURCE ON AS_USER.LDAP_SOURCE.LDAP_SOURCE_SEQ=AS_USER.APP_USER_LDAP_SOURCE.LDAP_SOURCE_SEQ") or die "Can't prepare 'av_sth': " . $DBI::errstr . "\n";
        $failedlog = $logpath."radsuite.user.activity.failed.log";
        $log = $logpath."radsuite.user.activity.log";
        $av_sth->execute();
        $| = 1;
        my $status;
        my $per;
        my $batch = 0;
        while ( @row = $av_sth->fetchrow_array() )
                {
              	$ctr++;
              	$per=int(0.5 + ($ctr/$entry_count)*100);
              	my $progress = sprintf("%3s", $per);
              	if ($batch == 1000)
              		{
              		print "\033[J"; # clear line
              		print "Status: ${progress}% Completed.[".("#" x $per).">".(" " x (99 - $per))."]"; # Print Status
              		print "\033[G"; # move cursor to the beginning of the line
              		$batch = 0;
              		}
              	$batch++;
              	foreach my $items (@row)
                        {
                        $row[$counter2]=$row[$counter2] || "-";
                        $counter2++;
                        }
                Logit_failed ("||||||$row[6]|$row[7]|$row[8]|$row[9]|$row[10]|$row[11]|$row[12]|$row[13]|$row[14]|$row[15]|$row[16]|No User ID found") unless ($row[0] ne '-' );
                my $counter2 = 0;
                
                my $line = neat_list( \@row, 150, "|");
                Logit ("$line");
                }
        # print final status for completion.
        print "\033[J"; # clear line
     	print "Status: ${per}% Completed.[".("#" x $per).(" " x (100 - $per))."]"; # Print Status
       	print "\033[G"; # move cursor to the beginning of the line
       	print "\n";
       	$av_sth->finish();
        unlink($pidfile);
        my @results;
        if (-e $failedlog.".0" )
        	{
        	@results = `ls -tr $failedlog*`;
        	print "\nFailed User Session Log(s):\n";
        	if (@results) {  foreach my $item (@results) {print "\t$item";}}
        	}
        if (-e $log.".0" )
        	{
        	@results = `ls -tr $log*`;
        	print "User Sesssion Log(s):\n";
        	if (@results) { foreach my $item (@results) {print "\t$item";}}
        	}
        }

# Close files
close LOG;

print "\nDone!\n";

############################################################################################
#
# Logit Failed Results File
#
############################################################################################

sub Logit_failed {
  open (FAILEDLOG, ">> $failedlog.$failedlogcounter") or die "LOG: $!\n";
  if (! $firstfailedlog)
  	{
  	$firstfailedlog = 1;
  	print_failed_header();		
  	}
  my ($what) = @_;
  print FAILEDLOG "$what\n";
  close FAILEDLOG;
  $checkfailedlogfilesize = -s $failedlog.".".$failedlogcounter;
  if ($checkfailedlogfilesize > 1073741824)
        {
        $failedlogcounter++;
   		open (FAILEDLOG, ">> $failedlog.$failedlogcounter") or die "LOG: $!\n";
  		print_failed_header();		
  		close FAILEDLOG;       
        }
}
sub print_failed_header
	{
	print FAILEDLOG "USERID|LASTNAME|FIRSTNAME|MIDDLENAME|ORGANIZATION|EMAILADDRESS|START|END|LASTACCESSED|IPADDRESS|GROUPNAME|LDAPUSERDESC|LDAPGROUPDESC|LDAPSOURCENAME|LDAPDOMAIN|LDAPDOMAINSUFFIX|LDAPDESC|ISSUE\n";
	}
############################################################################################
#
# Logit Results File
#
############################################################################################

sub Logit {
  open (LOG, ">> $log.$logcounter") or die "LOG: $!\n";
  if (! $firstlog)
  	{
  	$firstlog = 1;
  	print_header();		
  	}
  my ($what) = @_;
  print LOG "$what\n";
  close LOG;
  $checklogfilesize = -s $log.".".$logcounter;
  if ($checklogfilesize > 1073741824)
        {
        $logcounter++;
    	open (LOG, ">> $log.$logcounter") or die "LOG: $!\n";
    	print_header();
  		close LOG;
        }
}
sub print_header
	{
	print LOG "USERID|LASTNAME|FIRSTNAME|MIDDLENAME|ORGANIZATION|EMAILADDRESS|START|END|LASTACCESSED|IPADDRESS|GROUPNAME|LDAPUSERDESC|LDAPGROUPDESC|LDAPSOURCENAME|LDAPDOMAIN|LDAPDOMAINSUFFIX|LDAPDESC\n";
	}
############################################################################################
#
# Open_Database
#
############################################################################################
sub Open_Database {
  my ($usr, $pwd) = @_;

  # Set the ORACLE_HOME variable
  do {
    my $OH = `grep DARC /etc/oratab`;
    my ($db, $oh, $as) = split /:/, $OH;
    $ENV{ORACLE_HOME} = $oh;
  } unless $ENV{ORACLE_HOME};

  $dbh = DBI->connect("dbi:Oracle:host=darc-db;sid=DARC;port=1521", "$usr", "$pwd", { PrintError => 1 } ) ;
  do {
    Logit ("TERMINAL: Could not connect to database: $DBI::errstr");
    die;
  } unless $dbh;

}

############################################################################################
#
# Close_Database
#
############################################################################################
sub Close_Database {
  $dbh->disconnect() ;
}

############################################################################################
#
# Scram
#
############################################################################################
sub Scram {
        my $error;
        if ($_)
                {
                my $error = $_;
                Logit ('Script died: $error');
                unlink($pidfile);
                }
        else
                {
                Logit ('Script was killed');
                unlink($pidfile);
                }
        Close_Database();
        my @results = `ls $failedlog*`;
        print "Failed Activities Logs:\n";
        if (@results) {  foreach my $item (@results) {print "\t$item\n";}}
        @results = `ls $log*`;
        print "Activities Logs:\n";
        if (@results) { foreach my $item (@results) {print "\t$item\n";}}
        die;
}

############################################################################################
#
# Check for existing process
#
############################################################################################

sub check_for_existing_process
        {
        $pidfile = "/var/run/radsuite_user_logs.pid";
        if (-e $pidfile)
                {
                print "Process already running or stale pid file exists $pidfile\n";
                exit;
                }
        if (-e "/var/run/radsuite_qc_logs.pid")
                {
                print "Another Audit log process already running or stale pid file exists /var/run/radsuite_qc_logs.pid\n";
                exit;
                }
        if (-e "/var/run/radsuite_activity_logs.pid")
                {
                print "Another Audit log process already running or stale pid file exists /var/run/radsuite_activity_logs.pid\n";
                exit;
                }
        system("echo $$ > $pidfile");
        }