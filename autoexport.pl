#!/usr/bin/perl -w

use strict;
use DBI;
use IO::Handle qw( );
use Getopt::Long;

our ( $help, $throttle, $numtoupdate, $interval, $studynote, $series, $db, $aetitle, $propfile, $inputf, $remote_source, $task_type, $level, $insert_sth, $isUID, $addUID, $export_sth, $fh, $notify, @uids);
my $basedir            = "/opt/emageon/manager/rs-tools/";
my $logdir             = $basedir . "/log/";
my $log                = $basedir . "/log/autoexport.log";
my $PIDFILE            = "/var/run/autoexport.pid";

foreach ( "HUP", "INT", "QUIT", "KILL" ) {
   $SIG{$_} = \&cleanUp;
}

sub setStatus{
   open PID_F, '>', $PIDFILE or die "Can't create status file: $!\n";
   print PID_F $$;
   close PID_F;
   return;
}

sub getStatus{
    if ( -e $PIDFILE) {
        open PID_F, '<', $PIDFILE or die "Can't read status file: $!\n";
        my $PID = <PID_F>; chomp($PID);
        return $PID if kill 0, $PID;
    }
    return;
}

## Check if another instance is already running
my $running = &getStatus;
die "$0 is already running [ $running ]\n" if ( $running );
## Run it if you survived getStatus
&setStatus;

## Usage
sub usage() {
    print "\n";
    print "  This script is used to export un-ommitted secondary series and studynotes to Archive based on the following parameters. \n";
    print "  Usage: $0 [options]\n\n";
    print "  --studynote\t\t\tExport StudyNote Reports .\n";
    print "  --series\t\t\tExport Uncommitted Secondary Modalities (PR/KO/AU/OT) but not StudyNote Reports\n";
    print "  --db\t\t\t\tSpecifies if SQL query should be run to find objects to export\n";
    print "  --file [filename]\t\tPass a file containing the StudyNote Report SOPInstnaceUIDs one per line or for Series one series UIDS per line. \n";
    print "  --throttle\t\t\tSpecifies number of objects to load in one iteration, must be an Integer (Default Value : 50)\n";
    print "  --interval\t\t\tSpecifies time interval to check routerwork (in seconds), must be Integer (Default Value : 30) \n";
    print "  --help\t\t\tDisplay this help and exit\n\n";
    print "\n";
    exit 1;
}

## Get everything that is passed to the script
usage() if ( @ARGV < 1 or
     !GetOptions(
            "help"              => \$help,
            "studynotes"        => \$studynote,
            "series"            => \$series,
            "file=s"            => \$inputf,
            "db"                => \$db,
            "throttle:i"        => \$throttle,
            "interval:i"        => \$interval
     )
     or defined $help );

## Default values of some if they are not passed.
$throttle = $throttle || "50";
$interval = $interval || "30";
if ($series){
	$task_type = "EXPORT";
	$level = "SERIES";
}
if (defined $studynote){ 
	$task_type = "SN_EXPORT"; 
	$level = "IMAGE";
}

print "\n";
## Validate parameters

if ( defined $series && defined $studynote ) {
   print "Can't export StudyNotes and Uncommitted Series at the same time.\n";
   &usage;
}

if ( defined $studynote && !( defined $db || defined $inputf )) {
   print "--db or --file must be passed for the StudyNote Export to work\n";
   &usage;
}

if ( ! defined $series && ! defined $studynote ) {
   print "One of --studynote or --series parameters must be passed\n";
   &usage;
}

if ( defined $series && !( defined $db || defined $inputf )) {
   print "--db or --file must be passed for the StudyNote Export to work\n";
   &usage;
}

## Open the log file
unless(-d $logdir or mkdir $logdir ) { die "Unable to create $logdir\n"};
open LOG, '>>', $log or die "Can't create log file: $!\n";
LOG->autoflush(1);
my $ofh = select LOG;
$| = 1;
select $ofh;

sub LogIt {
   my $time_stamp = `date +"%F %T,%3N"`; chomp($time_stamp);
   my $msg = shift;
   if ( defined $log ) { print LOG "$time_stamp : $msg" or die "$!\n" };
   print "$time_stamp : $msg";
   $| = 1;
   return;
}

if ( -f "/etc/ora.env" ) {
   chomp( my $oraenv = `. /etc/ora.env;set | egrep "^ORA|^NLS|^TNS"` );
   my @ORAenv = split "\n", $oraenv;
   foreach (@ORAenv) {
      my ( $k, $v ) = split '=';
      $ENV{$k} = $v;
   }
} else {
   warn "Can't get Oracle configuration info.\n";
}

my $av_dbh = DBI->connect( "dbi:Oracle:host=darc-db;sid=DARC;port=1521", "UV_DICOM", "UV_DICOM", {AutoCommit => 1, PrintError => 1, RaiseError => 1} ) or die "Unable to connect\n";

&deleteTempTable;
&setupTempTable;
( $remote_source ) = $av_dbh->selectrow_array("SELECT DISTINCT REMOTE_SOURCE_KEY FROM UV_PRIMARY_SOURCE WHERE SERVICE = 'CSTORESCP'");
if( ! defined $remote_source ){
   print "Error:  There is no remote source configured for the archive in the primary source table.\n";
        exit 1;
} else {
   LogIt "Remote Source Key: $remote_source\n";
}


#Temp Table Queries
my $CheckListSQL = "SELECT COUNT(UIDS) FROM AUTOEXPORT_LIST WHERE UIDS = ?";
my $CollectAtmpts= "INSERT INTO AUTOEXPORT_LIST UIDS VALUES(?)";

# Task Queryies
my $SeriesSQL    = "SELECT SERIESINSTANCEUID FROM SERIES WHERE SERIESSTATE = 0 AND ARCHIVESTATE != 2 AND MODALITY IN ('PR','KO','AU','OT')";
my $StudyNoteSQL = "SELECT SOPINSTANCEUID FROM STUDYNOTEREPORT USR WHERE NOT EXISTS ( SELECT 1 FROM CLARCDBO.OBJECT CSR WHERE USR.SOPINSTANCEUID = CSR.SOPINSTANCEUID)";
my $InsertSQL    = "INSERT INTO TASK_QUEUE VALUES (QUEUE_KEY_SEQ.NEXTVAL, $remote_source, ?, '$level', '0', '$task_type', 'QUEUED', 1, 3, 60, SYSDATE, 3, 0, 0, 'Autocommit', null)";
my $TaskQSQL     = "SELECT COUNT(1) FROM TASK_QUEUE WHERE TASK_STATUS IN ('QUEUED','INPROGRESS')";
my $GetFailedSQL = "SELECT OBJECT_ID FROM TASK_QUEUE where TASK_STATUS = 'FAILED'";
my $DeleteSQL    = "DELETE FROM TASK_QUEUE WHERE TASK_STATUS = 'FAILED'";

LogIt "Checking current task queure failures.\n";
&deleteFailed;

sub setupTempTable
	{
	my $CreateTempTblSQL = "CREATE GLOBAL TEMPORARY TABLE AUTOEXPORT_LIST ( UIDS CHAR(64) ) ON COMMIT DELETE ROWS";	
	$av_dbh->do($CreateTempTblSQL) or die "Couldn't execute statement: " . $av_dbh->errstr;
	LogIt "Temporary progress table created.\n";
	}
	
sub deleteTempTable
	{
	my $CheckForTempTBLSQL = "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'AUTOEXPORT_LIST'";
	my $FindTempTbl = $av_dbh->prepare($CheckForTempTBLSQL);
    $FindTempTbl->execute() or die "Couldn't execute statement: " . $isUID->errstr;
    my $found = $FindTempTbl->fetchrow_array();
    $FindTempTbl->finish;
	if ($found)
		{
		my $DeleteTempTbl= "
		declare
   		c int;
		begin
   			select count(*) into c from user_tables where table_name = upper('AUTOEXPORT_LIST');
   			if c = 1 then
      			execute immediate 'drop table AUTOEXPORT_LIST';
   			end if;
		end;";
		$av_dbh->do($DeleteTempTbl) or die "Couldn't execute statement: " . $av_dbh->errstr;
		LogIt "Temporary progress table dropped.\n";
		}
	}
	
sub cleanUp {
   my $param = shift;
   my $now = scalar(localtime);
   if ( defined $export_sth ) { $export_sth->finish };
   if(defined $fh && tell($fh)) { close $fh };
   &deleteTempTable;
   $av_dbh->disconnect;
   unlink($PIDFILE);
   defined $param && LogIt "Received signal \"" . $param . "\".\n";
   close(LOG);
   exit;
	}

sub status {
   my ( $numTasks ) = $av_dbh->selectrow_array($TaskQSQL) or die "Couldn't execute statement: " . $av_dbh->errstr;
   
   if ( $numTasks >= $throttle ) {
      if(@uids){ 
      		{
      		foreach my $item (@uids)
      			{
      			LogIt "Exported: $item\n"
      			} 	
      		}
      	};
      LogIt "Number of active tasks is more than the throttle limit.\n";
      $notify = 1;
      @uids = ();
      return;
   }
   &deleteFailed();
   return 1;
}

sub deleteFailed
	{
    my $FailedQRY = $av_dbh->prepare($GetFailedSQL);
    $FailedQRY->execute() or die "Couldn't execute statement: " . $FailedQRY->errstr;
    while (my @row = $FailedQRY->fetchrow_array())
      	{
      	LogIt "Failed Export of Item: $row[0]\n";
      	}
    $FailedQRY->finish;   
    my $numdeleted = $av_dbh->do($DeleteSQL) or die "Couldn't execute statement: " . $av_dbh->errstr; chomp($numdeleted);
    if ( defined $numdeleted && $numdeleted ne '0E0' ){
      LogIt "Number of current failed tasks deleted: $numdeleted\n";
      }
    else
    	{
        LogIt "No failed tasks found.\n";
    	}		
	}

sub process {
   LogIt "Begin Processing...\n";
   my ($line, $uid);
   if ( defined $db ) {
      $uid = $export_sth->fetchrow_array;
   } elsif ( defined $inputf ) {
      $line = <$fh>;
      if ( defined $line ){
         chomp $line;
         $uid = $line;
      }
   }
   if ( defined $uid ) {
      chomp($uid);
      push(@uids, $uid);
      $isUID = $av_dbh->prepare($CheckListSQL);
      $isUID->execute($uid) or die "Couldn't execute statement: " . $isUID->errstr;
      my $found = $isUID->fetchrow_array();
      $isUID->finish;
      if(!$found)
      	{
      	$insert_sth->execute($uid) or die "Couldn't execute statement: " . $insert_sth->errstr;
      	}
      $addUID = $av_dbh->prepare($CollectAtmpts);
      $addUID->execute($uid) or die "Couldn't execute statement: " . $addUID->errstr;
      $addUID->finish; 
   } else {
      if(@uids) { 
      		{
      		foreach my $item (@uids)
      			{
      			LogIt "Exported: $item\n"
      			} 	
      		}
      	};
      LogIt "Finished Processing...\n";
      return;
   }
   return 1;
}



$insert_sth = $av_dbh->prepare_cached($InsertSQL);
$notify = 1;
if ( defined $db ) {
   if ( defined $series ) {
      $export_sth = $av_dbh->prepare($SeriesSQL);
   } elsif ( defined $studynote ) {
      $export_sth = $av_dbh->prepare($StudyNoteSQL);
   }
   $export_sth->execute() or die "Couldn't execute statement: " . $export_sth->errstr;
} elsif ( $inputf ) {
   open ($fh, '<', $inputf) or die $!;

}

while (42) { #http://en.wikipedia.org/wiki/42_(number)
   if ( status() ) {
      unless ( process() ){ last };
   } else {
      LogIt "Sleeping for $interval seconds\n";
      sleep($interval);
   }
}

&cleanUp;
