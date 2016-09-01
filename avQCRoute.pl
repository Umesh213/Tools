#!/usr/bin/perl -w

use strict;
use DBI;
use IO::Handle qw( );
use Getopt::Long;

our ( $help, $router_throttle, $qc_throttle, $numtoupdate, $lta_ae, $av_state, $interval, $totalRows, $propfile, $partition, $diskthreshold);
my $basedir            = "/opt/emageon/manager/rs-tools/";
my $logdir             = $basedir . "/log/";
my $log                = $basedir . "/log/avQCRoute.log";
my $PIDFILE            = "/var/run/avQCRoute.pid";

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
    print "  This script is used to route studies from AV to Archive based on the following parameters. \n";
    print "  Usage: $0 [options]\n\n";
    print "  -s, --state\t\t\tSpecifies the ROUTE_STATE value of the items in AV QC Router backlog. It is a required parameter.\n";
    print "  -l, --ltaae\t\t\tSpecifies the AETitle of the LTA. Only needed if we want to monitor routing from FIC to LTA\n";
    print "  -r, --router\t\t\tSpecifies the maximum number of Routerwork items in PENDING state for the LTA destination.\n\t\t\t\tOnly needed if --ltaae is defined, must be an Integer (Default Value : 5000)\n\n";
    print "  -q, --qc\t\t\tSpecifies the maximum number of items allowed in the AV QC Router, must be an Integer (Default Value : 50)\n";
    print "  -n, --numtoupdate\t\tSpecifies number of objects to load in one iteration, must be an Integer (Default Value : 30)\n";
    print "  -i, --interval\t\tSpecifies time interval to check routerwork (in seconds), must be Integer (Default Value : 120) \n";
    print "  -h, --help\t\t\tDisplay this help and exit\n\n";
    print "\n";
    exit 1;
}

## Get everything that is passed to the script
usage() if ( @ARGV < 2 or
     !GetOptions(
            "help"            => \$help,
            "state=i"     => \$av_state,
            "ltaae=s"   => \$lta_ae,
            "router=i"        => \$router_throttle,
            "qc=i"   => \$qc_throttle,
            "numtoupdate=i"   => \$numtoupdate,
            "interval:i"      => \$interval,
     )
     or defined $help );

## Default values of some if they are not passed.
$qc_throttle = $qc_throttle || "50";
$interval = $interval || "120";
$numtoupdate = $numtoupdate || "30";
#$propfile = "/opt/emageon/manager/etc/emageon.properties";
$partition = `awk -F= '/QCStudyRouterTempDir/{print \$NF}' /opt/emageon/manager/etc/emageon.properties`; chomp($partition);
$diskthreshold = 90;

print "\n";
## Validate parameters
if ( ! defined $av_state || $av_state <= 1 ) {
   print "State (AV QC ROUTE_STATE value) must be defined and must be > 1\n";
   &usage;
}

if ( ! defined $lta_ae && defined $router_throttle ) {
   print "Router Throttle can not be set without setting LTA AETitle\n";
   &usage;
}

if ( defined $lta_ae ) { $router_throttle = $router_throttle || "5000"};

my $select_sql_backlog = "SELECT count(1) FROM STUDY S WHERE S.ROUTE_STATE = $av_state";
my $select_sql_total   = "select count(st.STUDYINSTANCEUID)
                          from STUDY st, PATIENT pt
                          where pt.PATIENTDBNO = st.PATIENTDBNO
                          and st.ROUTE_STATE = 0
                          and st.ANONYMIZED = 0
                          and not exists (select 1 from SERIES ser where ser.STUDYINSTANCEUID = st.STUDYINSTANCEUID and ser.SERIESSTATE = 1)
                          and not exists (select 1 from GENERIC_OBJECT_QUEUE gq where (gq.STUDYINSTANCEUID = st.STUDYINSTANCEUID or (gq.STUDYINSTANCEUID is null and gq.PATIENTDBNO = st.PATIENTDBNO)) and gq.QUEUE_TYPE = 'DO_NOT_ROUTE')" ;
my $update_sql         = "update study set ROUTE_STATE = 0 where ROUTE_STATE = $av_state and rownum <= $numtoupdate";

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
   print LOG "$time_stamp : $msg" or die "$!\n";
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

my $arch_dbh = DBI->connect( "dbi:Oracle:host=darc-db;sid=DARC;port=1521", "clarcdbo/powers069" ) or die "Unable to connect\n";
my $av_dbh = DBI->connect( "dbi:Oracle:host=darc-db;sid=DARC;port=1521", "UV_DICOM/UV_DICOM" ) or die "Unable to connect\n";

if ( ! defined $lta_ae ) { LogIt "LTA Router monitoring has been disabled since the LTA AETitle is not provided\n"};

sub cleanUp {
   my $param = shift;
   my $now = scalar(localtime);
   $av_dbh->disconnect;
   $arch_dbh->disconnect;
   unlink($PIDFILE);
   defined $param && LogIt "Received signal \"" . $param . "\".\n";
   close(LOG);
   exit;
}

sub status {
   my $use = `df -mPH $partition | tail -1 | awk '{print \$(NF-1)}'`;chomp($use);
   $use =~ s/\%//g;chomp($use);
   LogIt "Available space on $partition is $use%\n";
   if ( $use > $diskthreshold ) {
      LogIt "Disk utlization of the AV QC Temp partition is above the critical threshold ($diskthreshold%).\n";
      return;
   }
   ### How big is the backlog
   my $hold = $av_dbh->prepare($select_sql_backlog);
   $hold->execute() or die "Couldn't execute statement: " . $hold->errstr;
   my @rowset = $hold->fetchrow_array();
   $hold->finish;
   $totalRows = $rowset[0];
   LogIt "Total number of studies to process : $totalRows\n";
   undef(@rowset);
   if ( defined $lta_ae ){
      ### Get the number of images pending to route to LTA since we don't want to create a backlog to the LTA
      my $select_sql_pend    = "select count(1) from routerwork where DESTINATIONAE = \'$lta_ae\' and STATUS = \'PENDING\'";
      my $pending_lta = $arch_dbh->prepare($select_sql_pend);
      $pending_lta->execute() or die "Couldn't execute statement: " . $pending_lta->errstr;
      @rowset = $pending_lta->fetchrow_array();
      $pending_lta->finish;
      my $pend_lta = $rowset[0];
      LogIt "Number of rows pending to route to LTA is $pend_lta\n";
      if ( $pend_lta > $router_throttle ) {
         LogIt "$pend_lta ( > $router_throttle ) routerwork tasks still pending for $lta_ae.\n";
         return;
      }
      undef(@rowset);
   }
   ### Get the number of studies still in the QC Router queue
   my $pending_av = $av_dbh->prepare($select_sql_total);
   $pending_av->execute() or die "Couldn't execute statement: " . $pending_av->errstr;
   @rowset = $pending_av->fetchrow_array();
   $pending_av->finish;
   my $pend_av = $rowset[0];
   LogIt "Number of rows pending to to get processed by QC router is $pend_av\n";
   if ( $pend_av > $qc_throttle ) {
      LogIt "$pend_av ( > $qc_throttle ) studies still pending in QC Router.\n";
      return;
   }
   undef(@rowset);
   return 1;
}

sub nap(){
   LogIt "Sleeping for $interval seconds\n";
   sleep($interval);
}

while ( ! defined $totalRows || $totalRows > 0 ){
   if ( status() ) {
      my $update = $av_dbh->prepare_cached($update_sql);
      $update->execute() or die "Couldn't execute statement: " . $update->errstr;
      my $num_updated = $update->rows;
      LogIt "Number of records updated : $num_updated \n";
      $totalRows-=$num_updated;
      if ( $totalRows <= 0 ) {
         LogIt "Finish\n";
         &cleanUp;
      }
   }
   &nap;
}