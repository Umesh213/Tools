#!/usr/bin/perl
 
use strict;
use File::Find;
use File::Path;
use File::Copy;
use DBI;
use Getopt::Long;
 
my ($help, @indir, $infile, %studyHash);
my $PIDFILE = "/var/run/uvplugincleanup.pid";
my $odir    = "/opt/emageon/backup/missing_images/";
my @files  = ();
my ( $count, $dbh, $avdbh, $sth, $r_sth, $avsth, %SopHash, $sState, $cTime);
 
use constant failure => 1;
use constant success => 0;
$count = 0;
 
## Making some space for the argument related errors to be clearly visible
print "\n";
 
## Handle the kill signals
foreach ( "HUP", "INT", "QUIT", "KILL" ) {
   $SIG{$_} = \&cleanUp;
}
 
# Set a state-file in the /var/run direcotry
sub setStatus{
   open PID_F, '>', $PIDFILE or die "Can't create status file: $!\n";
   print PID_F $$;
   close PID_F;
   return;
}
 
# Check if the state-file exists
sub getStatus{
   if ( -e $PIDFILE) {
       open PID_F, '<', $PIDFILE or die "Can't read status file: $!\n";
       my $PID = <PID_F>;
       return $PID if kill 0, $PID;
   }
   return;
}
 
## Check if another instance is already running
my $running = &getStatus;
die "$0 is already running [ $running ]\n" if ( $running );
## Run it if you survived getStatus
&setStatus;
 
# Usage 
usage() if ( @ARGV < 1 or
    ! GetOptions(
       'help|h|?' => \$help,
       'file=s' => \$infile,
       'dir=s' => \@indir,
       'odir=s' => \$odir
    ) or defined $help );
 
sub usage(){
  print "Unknown option: @_\n" if ( @_ );
  print "usage: program [--file file] [--dir directory] [--odir OutputDirectory] [--help|h|-?]\n\n";
  exit;
}
 
if (defined $infile && defined @indir){
  print "Can't specify both file and directory options together\n";
  usage();
}
 
# If a file is provided as an argument, load all the directories listed in the array
if (defined $infile){
  open(INDIR, "<$infile") or die "Can't open $infile for reading: $!\n";
  @indir = <INDIR>;
  close(INDIR);
}
 
# Make sure we have the environment variables required by Oracle
sub getOraEnv {
   open ORA, "<", "/etc/ora.env"
     or die "Can't source environment file: " . $! . "\n";
   while (<ORA>) {
       if ( m/=/ and !m/\A\#/ ) {
           chomp;
           my ( $k, $v ) = split /=/;
           $v =~ s/\$(\w+)/$ENV{$1}/g;
           ( $ENV{$k} = $v ) =~ s/export//;
           $ENV{$k} =~ s/[\"\s+]//g;
       }
   }
   close ORA;
   return;
}
 
sub openDB {
   $dbh = DBI->connect( "dbi:Oracle:host=darc-db;sid=DARC;port=1521", 'clarcdbo', 'powers069', { PrintError => 0, RaiseError => 1 } ) or die "Unable to connect to the database:\n\n" . $DBI::errstr . "\n\n";
   # Returns two columns : 1. SeriesState 2. [1] if AV record is more recent than the Archive, [0] otherwise.
   $sth = $dbh->prepare("select distinct uvsr.seriesstate , case when uvsr.update_dt > object.storedatetime then 1 else 0 end
                            from UV_DICOM.series uvsr, series, object
                            where uvsr.dcmseriesuid = series.seriesinstanceuid
                            and series.serieskey = object.serieskey
                            and sopinstanceuid = ?");
   # Is the object still in Routerwork
   $r_sth = $dbh->prepare("select 1 from routerwork where identifier = ?");
   $avdbh = DBI->connect( "dbi:Oracle:host=darc-db;sid=DARC;port=1521", 'UV_DICOM', 'UV_DICOM', { PrintError => 0, RaiseError => 1 } ) or die "Unable to connect to the database:\n\n" . $DBI::errstr . "\n\n";
   # Check to see if the object exists in AV and is local
   $avsth = $avdbh->prepare("select seriesstate from series where dcmseriesuid = ?");
   return;
}
 
sub timeStamp($) {
   my $s = shift;
   my $time_stamp = `date +"%F %T"`; chomp($time_stamp);
   print OF "$time_stamp: $s\n\n";
}
 
# For each directory passed as argument, find files and process
sub loadFiles() {
   foreach my $dir (@indir) {
      chomp($dir);
      if ( ! -d $dir ){
         print OF "Missing $dir\n";
         next;
      }
      print OF "Processing $dir ...\n";
      find({ wanted => \&process, no_chdir => 1 }, $dir);
      print OF "Removing empty directories from $dir ...\n";
      finddepth(sub{rmdir}, $dir)
   }
}
 
# process() is called for each file/direcrtory found by the find() method
sub process() {
  if ( ( -r ) && ( /\.dcm$/i ) && ( !-z ) ) {
     $count++;
     $sState = $cTime = undef;
     my $dcmFile = "$File::Find::name";
     my $dumpOutput = `/usr/local/dicom/bin/dcmdump $dcmFile 2>/dev/null | grep -iE '0010,0020|0008,0050|0008,0060|0008,0018|0020,000E' | sed 's/no value available/NULL/g' | awk '{print \$NF "|" \$3}' | tr -d "[]" | tr "\\n" "|"`;
     my %tagsValueH = split( "\\|", $dumpOutput );
     # Check if the image exists in EA and if AV is more recent
     my $ref = $dbh->selectrow_arrayref($sth, undef, $tagsValueH{"SOPInstanceUID"});
     if (defined $ref ) {
        ($sState, $cTime) = @$ref;
        chomp($sState);chomp($cTime);
     }else{
        # Do we have more than one record for the series, in which case we don't want to process
        my $avref = $avdbh->selectrow_arrayref($avsth, undef, $tagsValueH{"SeriesInstanceUID"});
        if (defined $avref){
           if(@$avref > 1) {
              print OF "ERROR|Ignoring $tagsValueH{\"SOPInstanceUID\"}, Multiple Series records found for $tagsValueH{\"SeriesInstanceUID\"}\n";
              return;
           }
           # Update the SeriesState if a record is found
           ($sState) = @$avref;
        }
     }
     print OF "$dcmFile|$tagsValueH{\"SOPInstanceUID\"}|$sState|$cTime";
     if ($cTime eq "0") {              # EA is more recent
        processFile("del", $dcmFile);
     } elsif ($sState eq "0") {        # AV is more recent 
        print MIS "Missing|$tagsValueH{\"SOPInstanceUID\"}|$tagsValueH{\"SeriesInstanceUID\"}|$tagsValueH{\"Modality\"}|$sState|$tagsValueH{\"AccessionNumber\"}|$tagsValueH{\"PatientID\"}|$dcmFile\n";
        my $studyUID = $File::Find::dir;
        $studyUID =~ s/.*StandardRoutableStudyInfo-//; chomp($studyUID);
        if( updateDB($studyUID) == success ) { processFile("del", $dcmFile) }
     } elsif (($sState eq "1") || (!defined $cTime)) {                       # Doesn't exists in EA and/or AV
        print MIS "Missing|$tagsValueH{\"SOPInstanceUID\"}|$tagsValueH{\"SeriesInstanceUID\"}|$tagsValueH{\"Modality\"}|$sState|$tagsValueH{\"AccessionNumber\"}|$tagsValueH{\"PatientID\"}|$dcmFile\n";
        processFile("mov",$dcmFile);
     }
     print OF "\n";
  }
  return;
}
 
# Delete or Move the file being processed
sub processFile(){
  my $op = shift;
  my $tmpFile = shift;
  my ($cmd, $status);
  my $router = $dbh->selectrow_array($r_sth, undef, $tmpFile);
  if ($router){
     print OF "|RouterworkIgnore";
     return failure;
  }
  if ($op eq "del"){
     if(unlink $tmpFile){
        print OF "|Deleted";
        return success;
     }else{
        print OF "|Delete Failed";
        return failure;
     }
  } elsif ($op eq "mov"){
     my @dirs = split /\//, $tmpFile;
     my $tmpDir = "$odir/$dirs[$#dirs-2]/$dirs[$#dirs-1]";
     my $fName = `basename $tmpFile`; chomp($fName);
     system("mkdir -p $tmpDir");
     if(move("$tmpFile","$tmpDir/$fName")){
        print OF "|Moved";
        return success;

     }else{
        print OF "|Move Failed";
        return failure;
     }
  }
}
 
# Reset the ROUTE_STATE flag in the study table
sub updateDB(){
  my $tmpUID = shift;
  my $update_sql = "update study set ROUTE_STATE = 0 where studyinstanceuid = ?";
  my $update = $avdbh->prepare_cached($update_sql);
  if( $update->execute($tmpUID) ){
     print OF "|Updated";
     return success;
  }else{
     print OF "|Update Failed";
     return failure;
  }
}
 
# Delete the state file and exit
sub cleanUp {
   my $param = shift;
   my $time_stamp = `date +"%F %T,%3N"`; chomp($time_stamp);
   unlink($PIDFILE);
   close(OF);
   close(OF);
   $dbh->disconnect;
   $avdbh->disconnect;
   defined $param && die "$time_stamp: Received signal \"" . $param . "\".\n";
   exit;
}
 
my $basedir = "/opt/emageon/manager/rs-tools";
my $logdir = $basedir . "/log/";
system("mkdir -p $logdir");
my $date = `date +%Y%m%d%H%M%S`; chomp($date);
my $output = $logdir . "cleanupUVPlugin." . $date . ".log";
my $missinglist = $logdir . "missing." . $date . ".txt";
open OF, '>', $output or die "Can't create log file: $!\n";
open MIS, '>', $missinglist or die "Can't create log file: $!\n";
my $fh = select(OF);
$| = 1;
select($fh);
 
print OF "Number of directories to cleanup: @indir\n";
print OF "Backup Direcotry is set to: $odir\n";
openDB();
timeStamp("StartFind");
loadFiles();    #call
print OF "Processed $count files\n";
timeStamp("StopFind");
cleanUp;
