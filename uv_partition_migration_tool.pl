#!/usr/bin/perl -w
# run the UV storage migration tool

#use strict;
use warnings;
use DBI;
use Cwd;
use File::Path;
use File::Copy;
use Getopt::Long;
use Scalar::Util qw(looks_like_number);
use Time::HiRes qw(time);
use vars qw($pidfile
			$noconfirm
			$nocleanupdirs
			$startmovetime
			$sourcevolume 
			$destinationvolume 
			$dbh 
			$counter 
			$sourcebasedir 
			$destinationbasedir 
			$destinationhighwatermark
			$log
			$parms
			$processed
			$deleted
			$status
			$sourcestatus
			$destinationstatus
			$parameter
			$quantity
			$loglevel
			$trace
			$timestamp
			$logtimestamp
			$useMenu
			$help
			$getoptresults
			$isallseries
			);

$timestamp=`date +%Y%m%d%H%M%S`;
chomp($timestamp);

# Set the signal interrups
$SIG{INT} = \&Scram;     # CTRL-C, kill -2
$SIG{QUIT} = \&Scram;    # CTRL-D, kill -3
$SIG{TERM} = \&Scram;    # kill or kill -15

# Program variables
$startmovetime =0;
$counter = 0;
$processed = 0;
$deleted = 0;
$status = 0;
$sourcestatus = 0;
$destinationstatus = 0;

check_for_existing_process();

if (!(-e '../log' and -d '../log'))
   {
   system ('mkdir ../log');
   	}
$log = "../log/uvpmt_activity_log_$timestamp.txt";


###########################################################################
#
# Execution Sequence
#
###########################################################################
eval 
	{
	$getoptresults = GetOptions ( 
			'debug|d' 			=> \$loglevel,
			'trace|t'			=> \$trace,
			'help|h' 			=> \$help,
			'no-confirm|n'		=> \$noconfirm,
			'nocleanup-dirs|c'	=> \$nocleanupdirs,
			'quantity|q=i'		=> \$quantity,
			'sourceid|s=i'		=> \$sourcevolume,
			'destinationid|r=i'	=> \$destinationvolume);
	};
if($@)
	{
	print "Incorrect usage: $@\n\n";
	parameter_printout();
	exit;
	}
if(!$getoptresults)
	{
	parameter_printout();
	exit;		
	}
if ($trace)
	{
	if ($loglevel){print "DEBUG = $loglevel\n";}
	if ($trace){print "TRACE = $trace\n";}
	if ($sourcevolume){print "SOURCEID = $sourcevolume\n";}
	if ($destinationvolume){print "DESTINATIONID = $destinationvolume\n";}
	if ($quantity){print "QUANTITY = $quantity\n";}	
	if ($noconfirm){print "NO-CONFIRM = $noconfirm\n";}
	}
check_for_help();

system("clear");
check_commandline_parameters();
#get_flags();
Main();
clean_exit();

###########################################################################
#
# Main program
#
###########################################################################

sub Main
	{
	if ($useMenu)
		{
		if ($trace)
			{
			print "Current menu state Status = $status\n";
			}
		if ($status == 1)
			{
			select_partition_options();
			validate_selected_partitions(); 
			}
		elsif ($status == 2)
			{
			get_partition_parameters();
			}
		elsif ($status == 3)
			{
			get_user_move_parameters();
			}
		elsif ($status == 4)
			{
			run_migration();
			if (!$nocleanupdirs and $isallseries)
				{
				cleanup_remaining_dirs();
				clean_exit();
				}
			else 
				{
				print "\nEither you have not chosen to move all series or you to not perform directory cleanup, so no cleanup will occur.\n";
				}
			clean_exit();
			}
		}
	else
		{
		validate_selected_partitions();
		get_partition_parameters();
		display_commandline_move_parameters();
		run_migration();
		if (!$nocleanupdirs)
			{
			cleanup_remaining_dirs();
			}
		clean_exit();
		}
	}

###########################################################################
#
# Check for Help flag
#
###########################################################################

sub check_for_help
        {
		if ($help)
        	{
        	parameter_printout();
        	unlink($pidfile);
  			exit;	
        	}

        }


########################################################################################################
#
# Parse Commandline paramters
#
#########################################################################################################

sub check_commandline_parameters
        {
        if ($trace)
			{
			print "Get start Commandline Parameter status = $status.\n";
			}
        if (!($sourcevolume and $destinationvolume and $quantity))
                {
                if ($loglevel)
                        {
                        Logit("DEBUG: No commandline parameters provided, proceeding with menu based execution.");
                        }
                $useMenu = 1;
                $status = 1;
                }
        elsif ($sourcevolume and $destinationvolume and $quantity)
                {
                
                if (!looks_like_number($sourcevolume) or !looks_like_number($destinationvolume) or !looks_like_number($quantity))
                        {
                        parameter_printout();
                        }
                $counter=$quantity;
                print "Source volume id used is: $sourcevolume\n";
                print "Destination volume id used is: $destinationvolume\n";
                print "Quantity of series to be moved: $quantity\n";
                if ($loglevel)
                        {
                        Logit("DEBUG: Source volume id used is: $sourcevolume");
                        Logit("DEBUG: Destination volume id used is: $destinationvolume");
                        Logit("DEBUG: Quantity of series to be moved: $quantity");
                        }
                else
                        {
                        Logit("Source volume id used is: $sourcevolume");
                        Logit("Destination volume id used is: $destinationvolume");
                        Logit("Quantity of series to be moved: $quantity");
                        }
                }
        elsif (!($sourcevolume and $destinationvolume))
                {
                if ($loglevel)
                        {
                        Logit("DEBUG: Source and Destination volume ids are both required for commandline execution.");
                        parameter_printout();
                        }
                else
                        {
                        parameter_printout();
                        }
                }
        elsif ($sourcevolume and $destinationvolume)
                {
                if (!looks_like_number($sourcevolume) or !looks_like_number($destinationvolume))
                        {
                        parameter_printout();
                        }
                               
                print "Source volume id used is: $sourcevolume\n";
                print "Destination volume id used is: $destinationvolume\n";
                print "Source and Destination volume ids are present.\n";
                if ($loglevel)
                        {
                        Logit("DEBUG: Source volume id used is: $sourcevolume");
                        Logit("DEBUG: Destination volume id used is: $destinationvolume");
                        Logit("DEBUG: No commandline quantity parameter provided with source and destination values, defaulting to all");
                        }
                else
                        {
                        Logit("Source volume id used is: $sourcevolume");
                        Logit("Destination volume id used is: $destinationvolume");
                        Logit("No commandline quantity parameter provided with source and destination values, defaulting to all");
                        }
				$isallseries = 1;	
	       		}        
    if ($trace)
		{
		print "Get end Commandline Parameter status = $status.\n";
		}	
	}
#############################################################################################
#
# Display commandline user move parameters results
#
#############################################################################################

sub display_commandline_move_parameters
    {
    Open_Database('UV_DICOM', 'UV_DICOM');
    my $seriescount = 0;
    my $destinationspaceremaining;
    my $sth = $dbh->prepare("select count(uvbasedir) from series where uvbasedir = '$sourcebasedir'") or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";
    ### Execute the statement in the database
    $sth->execute or die "Can't execute SQL statement: $DBI::errstr\n";
    ### Attempt to retrieve results
    $seriescount = $sth->fetchrow_array();
	$sth->finish;
	
    Close_Database();
    my @alldsspartitions = `df -m | grep dss | awk -F'\% ' '{ print \$2}'`;
    chomp @alldsspartitions;
    foreach my $checkdir (@alldsspartitions)
        {
        if ($destinationbasedir =~ $checkdir)
            {
            $destinationspaceremaining = `df -h | grep $checkdir | grep dss | awk -F% '{print \$1}' | awk '{print \$NF}'`;
            chomp($destinationspaceremaining);
            }
        }
	if ($seriescount == 0)
		{
        if ($loglevel)
            {
			Logit("DEBUG: There are no series available to be moved, program must exit now.");
			}
		print "There are no series available to be moved, program must exit now.\n";
		clean_exit();
        }
	print "There are $seriescount series in the source volume that are available to move to your destination volume.\n\n";
	print "The destination directory is: $destinationbasedir\t";
    print "There is $destinationspaceremaining% of the space used on the destination partition.\n\n";
	if ((looks_like_number($quantity)) and ($quantity <= 0))
		{
	    print "You have entered and invalid quantity value, there are only $seriescount series available to be moved.\nHit ENTER to return to the commandline.";
        if ($loglevel)
            {
            Logit("DEBUG: Quantity of series to move is less than or equal to zero, program is closing.");
            }
        else
            {
            Logit("Quantity of series to be moved: $quantity");
			}
		<STDIN>;
		clean_exit();
		}
    elsif (!$quantity)
        {
		$quantity = $seriescount;
        print "You have chosen to move all series. Hit ENTER to continue, otherwise hit Ctrl^C to abort.";
        if ($loglevel)
            {
			Logit("DEBUG: Entire volume will be moved, total number of series is $quantity");
			}
        else
            {
            Logit("Entire volume will be moved");
            }
        $quantity = $seriescount;
        }

	elsif ($quantity > $seriescount)
		{
	    print "You have entered and invalid quantity value, there are only $seriescount series available to be moved.\nHit ENTER to return to the commandline.";
        if ($loglevel)
            {
            Logit("DEBUG: Quantity of series to move exceeds series present on the source volume, program is closing.");
            }
        else
            {
            Logit("Quantity of series to be moved: $quantity");
			}
		<STDIN>;
		clean_exit();
		}
	else
        {
        print "You have chosen to move $quantity series. Hit ENTER to continue, otherwise hit Ctrl^C to abort.";
        if ($loglevel)
            {
            Logit("DEBUG: $quantity of $seriescount will be moved");
            }
        else
            {
            Logit("$quantity series will be moved");
            }
        }
	$counter=$quantity;
	if (! $noconfirm)
		{
		<STDIN>;
		}
	else
		{
		print "\"--NO-CONFIRM\" Flag used, bypassing user prompt.\n";
			
		}
    }

###################################################################################################
#
# Select partitions for the migration
#
###################################################################################################

sub select_partition_options
        {
        if ($trace)
			{
        	print "Select Partitions Start status = $status\n";
			}
        # Clear the screen for menu print out and selection
        system("clear");
        # Connect to the db
        Open_Database('UV_DICOM', 'UV_DICOM');

        #Get Partitions options for data move

        # Prepare DB Statement for MRN updates
        my @row;
        my $sth = $dbh->prepare("select Volume_id,available,prefix,description from image_volumes")
          or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";

        ### Execute the statement in the database
        $sth->execute or die "Can't execute SQL statement: $DBI::errstr\n";
        ### Retrieve the returned rows of data
        print "Below are the partitions available to the partition move tool:\n";
        print "Volume ID\tActive\t\tBase Directory\t\t\t\t\t\tDescription\n\n";

        my $item;
        while ( @row = $sth->fetchrow_array() )
                {
                my $line;
                foreach $item (@row)
                        {
                        if ($item)
                                {
                                $line .= $item."\t\t";
                                }
                        else
                                {
                                $line .= "No Value\t";
                                }
                        }
                chop($line);
                Logit ("Getting Partition Options: $line");
                print $line."\n";
                }
        print "\n";
        warn "Data fetching terminated early by error: $DBI::errstr\n" if $DBI::err;
        Close_Database();

        print "Type the ID of the source volume: ";
		$sourcevolume = <STDIN>;
        chomp ($sourcevolume);

        print "Type the ID of the destination volume: ";
		$destinationvolume = <STDIN>;
        chomp ($destinationvolume);
    
        }
        

sub validate_selected_partitions
	{
	if ($trace)
		{
		print "Validate partition status = $status\n";
		}
	if (!looks_like_number($sourcevolume))
        {
        $sourcevolume = 999999;
        }
	if (!looks_like_number($destinationvolume))
        {
        $destinationvolume = 999999;
        }
	$sourcestatus = 0;
	$destinationstatus = 0;
	print "Checking values of the selected volume IDs....\n";

		# checking for valid partition values
	if ($sourcevolume == $destinationvolume)
		{
		print "Source and Destination Partitions are the same or non-numeric values, hit return to go to the selection menu";
		<STDIN>;
		}
	else
		{
		checking_source_partition();
		checking_destination_partition();
		}
	if ($useMenu)
		{
		if (($sourcestatus == 1) and ($destinationstatus == 1))
			{
			$status = 2;
			Main();
			}
		else
			{
			$status = 1;
			Main();
			}
		}	

	sub checking_source_partition
        {
        if ($trace)
			{
        	print "checking source paritions status = $status\n";
			}
        Open_Database('UV_DICOM', 'UV_DICOM');
        my $sth = $dbh->prepare("select count(volume_id) from image_volumes where volume_id = $sourcevolume and available != 'Y'") 
			or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";

        ### Execute the statement in the database
        $sth->execute 
		or die "Can't execute SQL statement: $DBI::errstr\n";
        ### Attempt to retrieve results
        my @found;
        @found = $sth->fetchrow_array();
        if ( $found[0] == 1 )
                {
                print "\tSource Volume id is valid.\n";
                $sourcestatus = 1;
             	if($trace)
        			{
        			print "Source ID count found: $found[0]\n";	
        			}
				}
        else
                {
				if ($useMenu)
					{
					print "\tSource Volume ID is invalid or partition is still marked available, hit return to go to the selection menu.\n";
					Logit ("Invalid partition option(s) selected");
					<STDIN>;
					}
				else
					{
					print "\tSource Volume ID is invalid or partition is still marked available, hit ENTER to return to the commandline.\n";
					Logit ("Invalid partition option(s) selected");
					<STDIN>;
					clean_exit();
					}
                }
		$sth->finish;
        Close_Database();

        }
	sub checking_destination_partition
		{
		if ($trace)
			{
			print "checking destination paritions status = $status\n";
			}
        Open_Database('UV_DICOM', 'UV_DICOM');
        my $sth = $dbh->prepare("select count(volume_id) from image_volumes where volume_id = $destinationvolume") 
		or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";

        ### Execute the statement in the database
        $sth->execute 
		or die "Can't execute SQL statement: $DBI::errstr\n";
        ### Attempt to retrieve results
        my @found;
        @found = $sth->fetchrow_array();
        if ( $found[0] == 1 )
                {
                print "\tDestination Volume id is valid.\n\n";
				$destinationstatus = 1;
        		if($trace)
        			{
        			print "Destination ID count found: $found[0]\n";	
        			}
                }
        else
                {
                if ($useMenu)
                        {
						print "\tDestination Volume ID is invalid or marked unavailable, hit return to go to the selection menu.\n";
						Logit ("Invalid partition option(s) selected");
                        <STDIN>;

                        }
                else
                        {
                        print "\tDestination Volume ID is invalid or marked unavailable, hit ENTER to return to the commandline.\n";
						Logit ("Invalid partition option(s) selected");
						<STDIN>;
						clean_exit();
                        }
                }
		$sth->finish;
        Close_Database();
		}
	#unlink($pidfile);	
	if ($trace)
		{
		print "Select Partitions End status = $status\n";
		}
	}
	
############################################################################################
#
# Get partition parameters for the selected partitions
#
############################################################################################
	
sub get_partition_parameters
	{
	if ($trace)
			{
			print "get start partition parameters status = $status\n";
			}
	$sourcestatus = 0;
	$destinationstatus = 0;
	sub get_source_partition_parameters
        {
        if ($trace)
			{	
        	print "get Source partition parameters status = $status\n";
			}
        Open_Database('UV_DICOM', 'UV_DICOM');
        my $query = "select prefix from image_volumes where volume_id = $sourcevolume";
		if ($loglevel)
            {
			Logit("DEBUG: Source base directory query for volume_id $sourcevolume: $query");
			}		
		my $sth = $dbh->prepare($query) or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";
        ### Execute the statement in the database
        $sth->execute or die "Can't execute SQL statement: $DBI::errstr\n";
        ### Attempt to retrieve results
        $sourcebasedir = $sth->fetchrow_array();
		if ($loglevel)
            {
			Logit("DEBUG: Source base directory found: $sourcebasedir");
			}
		$sth->finish;
        Close_Database();
        print "Source Base Directory: ".$sourcebasedir."\n\n";
		$sourcestatus = 1;
        }
	sub get_destination_partition_parameters
		{
		if ($trace)
			{
			print "get destination partition parameters status = $status\n";
			}
		Open_Database('UV_DICOM', 'UV_DICOM');
		my $qry;
		my $sth = $dbh->prepare("select prefix from image_volumes where volume_id = ".$destinationvolume)
			or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";
		### Execute the statement in the database
		$sth->execute
			or die "Can't execute SQL statement: $DBI::errstr\n";
		### Attempt to retrieve results
		while ($qry = $sth->fetchrow_array())
				{
				$destinationbasedir = $qry;
				}
		$sth->finish;
		my $sth2 = $dbh->prepare("select high_watermark from image_volumes where volume_id = ".$destinationvolume)
				or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";
		### Execute the statement in the database
		$sth2->execute
				or die "Can't execute SQL statement: $DBI::errstr\n";
		### Attempt to retrieve results
		while ($qry = $sth2->fetchrow_array())
				{
				$destinationhighwatermark = $qry;
				}
		$sth2->finish;
		Close_Database();
		print "Destination Base Directory: ".$destinationbasedir."\n";
		print "Destination Directory Highwatermark: ".$destinationhighwatermark."\%\n";
		$destinationstatus = 1;
		}
	get_source_partition_parameters();
	get_destination_partition_parameters();
	
	if ($useMenu)
		{
		if (($sourcestatus == 1) and ($destinationstatus == 1))
			{
			$status = 3;
			Main();
			}
		else
			{
			$status = 1;
			Main();
			}
		}
	if ($trace)
		{
		print "get final partition parameters status = $status\n";
		}
	}


#############################################################################################
#
# Get menu user move parameters
#
#############################################################################################

sub get_user_move_parameters
    {
		$sourcestatus = 0;
		$destinationstatus = 0;
	
		Open_Database('UV_DICOM', 'UV_DICOM');
		my $seriescount = 0;
		my $destinationspaceremaining;
		my $sth = $dbh->prepare("select count(uvbasedir) from series where uvbasedir = '$sourcebasedir'") or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";
		### Execute the statement in the database
		$sth->execute or die "Can't execute SQL statement: $DBI::errstr\n";
		### Attempt to retrieve results
		$seriescount = $sth->fetchrow_array();
		if ($seriescount == 0)
			{
			if ($loglevel)
				{
				Logit("DEBUG: There are no series available to be moved starting over.");
				}
			print "There are no series available to be moved starting over. Hit Enter to continue.\n";
			<STDIN>;
			system("clear");
			}
		else 
			{
			$sourcestatus = 1;
			}
	
		$sth->finish;
		Close_Database();
		#system('clear');
		if ($sourcestatus == 1)
			{
			my @alldsspartitions = `df -m | grep dss | awk -F'\% ' '{ print \$2}'`;
			chomp @alldsspartitions;
			foreach my $checkdir (@alldsspartitions)
				{
				if ($destinationbasedir =~ $checkdir)
					{
					$destinationspaceremaining = `df -h | grep $checkdir | grep dss | awk -F% '{print \$1}' | awk '{print \$NF}'`;
					chomp($destinationspaceremaining);
					}
				}
			print "There are $seriescount series in the source volume that are available to move to your destination volume.\n\n";
			print "The destination directory is: $destinationbasedir\t\n";
			print "There is $destinationspaceremaining% of the space used on the destination partition.\n\n";
			print "To move all series hit enter, or enter the number of series you wish to move and hit enter: ";
			$quantity = <STDIN>;
			chomp($quantity);
			if ((looks_like_number($quantity)) and ($quantity <= 0))
				{
				print "You entered: $quantity\t This value is less than or equal to zero, hit ENTER to return to the commandline.";
				<STDIN>;
				}
			elsif (!$quantity)
				{
				print "You have chosen to move all series. Hit ENTER to continue, otherwise hit Ctrl^C to abort.";
				$counter = $seriescount;
				$quantity = $seriescount;
				$isallseries = 1;	
				if (! $noconfirm)
					{
					<STDIN>;
					}
				else
					{
					print "\"--NO-CONFIRM\" Flag used, bypassing user prompt.\n";
					}            
				$destinationstatus = 1;
				}
			elsif ((looks_like_number($quantity)) and ($quantity > $seriescount))
				{
				print "You entered: $quantity\t This valid value is too large, hit ENTER to return to the prior menu.";
				<STDIN>;
				}

			elsif ((!looks_like_number($quantity)))
				{
				print "You entered: $quantity\t This is not a valid value, hit ENTER to return to the prior menu.";
				<STDIN>;
				}
			elsif ((looks_like_number($quantity)) and ($quantity < $seriescount) and ($quantity >= 1) )
				{
				print "You entered: $quantity\t If you wish to move $quantity of $seriescount series, hit ENTER to continue, otherwise Ctrl^C to abort.";
				$counter = $quantity;
				
				if (! $noconfirm)
					{
					<STDIN>;
					}
				else
					{
					print "\n\"--NO-CONFIRM\" Flag used, bypassing user prompt.\n";
					}
				$destinationstatus = 1;
				}
			}
				
		if ($useMenu)
			{
			if (($sourcestatus == 1) and ($destinationstatus == 1))
				{
				$status = 4;
				Main();
				}
			else
				{
				$status = 1;
				Main();
				}
			}
	
	}
  
############################################################################################
#
# Run Migration using validated parameters
#
############################################################################################

sub run_migration
	{
	Open_Database('UV_DICOM', 'UV_DICOM');
	my $query = "select seriesinstanceuid from (select seriesinstanceuid from series where uvbasedir = (select prefix from image_volumes where volume_id = $sourcevolume)) where rownum <= $quantity";
    my $sth = $dbh->prepare($query)
		or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";
	if ($loglevel)
		{
		Logit("DEBUG: Migration query for source volume_id $sourcevolume: \t$query");
		}		
	$sth->execute or die "Can't execute SQL statement: $DBI::errstr\n";
	$startmovetime = time;
	while (my $candidate = $sth->fetchrow_array())
		{
		my $sth2;
		my $query2 = "select uvsubdir||studyinstanceuid||'/'||seriesinstanceuid||'/' from series where seriesinstanceuid = $candidate";
		eval
			{
			$sth2 = $dbh->prepare($query2);
			};
		if ($@)
			{
			Logit("Can't prepare query : $query2 : $DBI::errstr");
			clean_exit();
			}
		eval
			{
			$sth2->execute
			};
		if ($@)
			{
			Logit("Can't execute SQL statement $query2 : $DBI::errstr\n");
			clean_exit();
			}
		if ($loglevel)
			{
			Logit("DEBUG: Migration query for source series uid $candidate: \t$query2");
			}
		
		while (my $seriesdir = $sth2->fetchrow_array() and ($counter > 0))
			{
			my $destinationseriesmd5sum;
			my $sourceseriesmd5sum;
			my $sourcefilelist;
			my $isfile = 0;
			if (! -d $destinationbasedir.$seriesdir)
				{
				mkpath($destinationbasedir.$seriesdir);
				if (! -d $destinationbasedir.$seriesdir)
					{
					Logit("Failed to create directory: $destinationbasedir$seriesdir. Check user permission in the destination directory.");
					clean_exit();
					}	
				}
			eval 
				{
				if ( -d $sourcebasedir.$seriesdir)
					{
					opendir(DIR, $sourcebasedir.$seriesdir );
					while (my $filename = readdir(DIR)) 
						{
						if ($filename ne '..' and $filename ne '.')
							{
							$isfile = 1;
							}
						}
					closedir(DIR);
					}			
	
				if ($isfile == 1)
					{
					$sourceseriesmd5sum = `md5sum $sourcebasedir$seriesdir* | awk '{print \$1}'`;
					system("/bin/cp --force $sourcebasedir$seriesdir* $destinationbasedir$seriesdir");
					Logit("Copying series key $candidate from $sourcebasedir$seriesdir to $destinationbasedir$seriesdir");
					$destinationseriesmd5sum = `md5sum $destinationbasedir$seriesdir* | awk '{print \$1}'`;
					}
				};
				
			if ($@) 
				{
				Logit( "No files or for series key $candidate: " . $@ );
				}				

			if ($isfile == 0)
				{

				eval
					{
					if (-d $sourcebasedir.$seriesdir)
						{
						system("rmdir  -p --ignore-fail-on-non-empty $sourcebasedir$seriesdir 2>/dev/null");
						if ($loglevel)
							{
							Logit("DEBUG: Removing source directory $sourcebasedir$seriesdir for series uid $candidate");
							}						
						}
					};
				if ($@)                                    
					{
					Logit( "Can't delete source directory files in $sourcebasedir$seriesdir for series uid $candidate': " . $@ );
					clean_exit();
					}			
				Logit( "Deleted directory for series key $candidate");
				$deleted++;
				
				my $sth3;
        		eval 
					{
					my $query3 = "update series set uvbasedir = '$destinationbasedir' where seriesinstanceuid = $candidate";
					if ($loglevel)
						{
						Logit("DEBUG: Migration query for source directory for series uid $candidate: \t$query3");
						}						
					$sth3 = $dbh->prepare($query3);
					};
				if ($@) 
					{
					Logit( "TERMINAL: Can't prepare 'extract statement for seriesuid $candidate': " . $@ );
					clean_exit();
					}
				eval 
					{
					$sth3->execute;
					};
				if ($@)
					{
					Logit( "TERMINAL: Can't execute statement for seriesuid $candidate': " . $@ );
					clean_exit();
					}
				$sth3->finish;
				
				}
				
			if ($loglevel)
				{
				if ($sourceseriesmd5sum)
					{
					Logit("DEBUG: Source md5sums were\n$sourceseriesmd5sum");
					Logit("DEBUG: Destination md5sums were\n$destinationseriesmd5sum");
					}
				}
			if (($sourceseriesmd5sum and $destinationseriesmd5sum)and ($sourceseriesmd5sum eq $destinationseriesmd5sum))
				{
				
				if ($loglevel)
					{
					Logit("DEBUG: Comparison worked for series key: $candidate\n");
					}
				my $sth3;
        		eval 
					{
					my $query3 = "update series set uvbasedir = '$destinationbasedir' where seriesinstanceuid = $candidate";
					if ($loglevel)
						{
						Logit("DEBUG: Migration query for source directory for series uid $candidate: \t$query3");
						}						
					$sth3 = $dbh->prepare($query3);
					};
				if ($@) 
					{
					Logit( "TERMINAL: Can't prepare 'extract statement for seriesuid $candidate': " . $@ );
					clean_exit();
					}
				eval 
					{
					$sth3->execute;
					};
				if ($@)
					{
					Logit( "TERMINAL: Can't execute statement for seriesuid $candidate': " . $@ );
					clean_exit();
					}
				$sth3->finish;
				print ".";
				}

				eval
					{
					if (-d $sourcebasedir.$seriesdir)
						{
						system("/bin/rm --force $sourcebasedir$seriesdir* 2>&1");
						system("rmdir  -p --ignore-fail-on-non-empty $sourcebasedir$seriesdir 2>/dev/null");
						if ($loglevel)
							{
							Logit("DEBUG: Removing source directory $sourcebasedir$seriesdir for series uid $candidate");
							}						
						}
					};
				if ($@)                                    
					{
					Logit( "Can't delete source directory files in $sourcebasedir$seriesdir for series uid $candidate': " . $@ );
					clean_exit();
					}			
				$counter--;
				$processed++;
				}
			
		
		my @alldsspartitions = `df -m | grep dss | awk -F'\% ' '{ print \$2}'`;
		my $destination_used;
		chomp @alldsspartitions;
		foreach my $checkdir (@alldsspartitions)
			{
			if ($destinationbasedir =~ $checkdir)
				{
				$destination_used = `df -h | grep $checkdir | grep dss | awk '{ print \$5}'`;
				chomp($destination_used);
				chop($destination_used);
				}
        	}
		Open_Database('UV_DICOM', 'UV_DICOM');
		my $query4 = "select high_watermark from image_volumes where volume_id = $destinationvolume";
		my $sth4 = $dbh->prepare($query4)
			or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";
		
		$sth4->execute or die "Can't execute SQL statement: $DBI::errstr\n";
		my $highwatermark_now = $sth4->fetchrow_array();
		$sth4->finish;
		if ($highwatermark_now <= $destination_used)
			{
			print "\n\nThe Highwatermark threshold for the destination partition has been exceeded, The program will now need to exit.\n";
			Logit("The Highwatermark threshold for the destination partition has been exceeded, The program will now exit.\n");
			if ($loglevel)
				{
				Logit("DEBUG: Current watermark value of $highwatermark_now is now less than or equal to the remaining destination storage space of $destination_used.\n");
				}
			$sth->finish;
			$sth2->finish;
			Close_Database();
			clean_exit();
			}
		}
	Close_Database();
	}

############################################################################################
#
# Cleanup remaining directories
#
############################################################################################	
sub cleanup_remaining_dirs
	{
	my @alldsspartitions = `df -m | grep dss | awk -F'\% ' '{ print \$2}'`;
	my $destination_total;
	my $destination_rem;
	my $source_used;
	chomp @alldsspartitions;
	foreach my $checkdir (@alldsspartitions)
		{
		if ($sourcebasedir =~ $checkdir)
			{
			my $line = `df -m | grep $checkdir | grep dss`;
			chomp($line);
			print "\nDoes $sourcebasedir contain $checkdir $line\n";
			$source_used = `df -m | grep $checkdir | grep dss | awk '{ print \$3}'`;
			chomp($source_used);
			}
		if ($destinationbasedir =~ $checkdir)
			{
			my $line = `df -m | grep $checkdir | grep dss`;
			chomp($line);
			print "Does $destinationbasedir contain $checkdir $line\n";
			$destination_rem = `df -m | grep $checkdir | grep dss | awk '{ print \$4}'`;
			chomp($destination_rem);
			}
	   	}
	print "Source Used:            $source_used MBs\n";
	print "\nDestination Remaining:  $destination_rem MBs\n";
	print "\n";
	if ($source_used > $destination_rem)
		{
		print "The destination partition space remaining is less than the space still required by the source partition.\n";
		print "The program cannot continue due to this space constraint.\n";
		print "If you would like to proceed with the transfer even though there may not be enough space, please enter Y to continue.\n";
		print "Otherwise any other key to skip this step.\n";
		if (!$noconfirm)
			{
			my $prompt = <STDIN>;
			chomp($prompt);
			if ($prompt !~ /^[Yy]/)
				{
				print "Exiting as requested.\n"; 
				clean_exit();
				}
			else 
				{
				print "Continuing as requested.\n";	
				}
			}
		else
			{
			print "NO-CONFIRM Flag used, bypassing user prompt.\n";	
			}
		}
	Open_Database('UV_DICOM', 'UV_DICOM');
	my $query4 = "select high_watermark from image_volumes where volume_id = $destinationvolume";
	my $sth4 = $dbh->prepare($query4)
		or die "Can't prepare 'extract statement': " . $DBI::errstr . "\n";
	
	$sth4->execute or die "Can't execute SQL statement: $DBI::errstr\n";
	my $highwatermark_now = $sth4->fetchrow_array();
	if ($sth4){$sth4->finish();}
	Close_Database();	
	if ($isallseries)
		{
		print "\nDeleting all empty directories before attempting Rsync.\n";
		while (@results)
			{
			@results = `find $sourcebasedir -type d -empty -print`;
			foreach my $item (@results)
				{
				chomp($item);
				Logit("Deleting empty source directory: $item");
				system("rmdir $item");
				}
			}
		}
		print "\nCopying any residual unidentified files from Source partition $sourcebasedir to $destinationbasedir\n";
		system("/usr/bin/rsync --remove-source-files --progress -EXogqprcu --progress $sourcebasedir $destinationbasedir >> $log");
		@results = `find $sourcebasedir -type d -empty -print`;
		while (@results)
			{
			@results = `find $sourcebasedir -type d -empty -print`;
			foreach my $item (@results)
				{
				chomp($item);
				Logit("Deleting empty source directory: $item");
				system("rmdir $item");
				}
			}
		print "\nDeleting all remaining empty directories after attempting Rsync.\n";
		@results = `find $sourcebasedir -type d -empty -print`;
		while (@results)
			{
			@results = `find $sourcebasedir -type d -empty -print`;
			foreach my $item (@results)
				{
				chomp($item);
				Logit("Deleting empty source directory: $item");
				system("rmdir $item");
			}
		}
	}

	
############################################################################################
#
# Summary printout
#
############################################################################################

sub summary
    {
	print "\nSummary of Events\n\n";
	my $scriptendtime = time;
	my $totaltime = sprintf("%.1f", ($scriptendtime - $startmovetime));
	my $averagemovetime;
	my $moved = 0;
	if ($processed != 0)
		{
		$averagemovetime = sprintf("%.2f", (($scriptendtime - $startmovetime)/$processed));
		$moved = ($processed - $deleted);
		print "\t$moved series were moved from volume $sourcevolume to $destinationvolume.\n";
		Logit("$moved series were moved from volume $sourcevolume to $destinationvolume.");
		}
	else
		{
		print "\tNo series were moved from volume $sourcevolume to $destinationvolume.\n";
		Logit("No series were moved from volume $sourcevolume to $destinationvolume.");		
		}

	if ($deleted > 0)
		{
		print "\t$deleted series directories were removed if present due to missing files (no copying occurred).\n";
		Logit("$deleted series directories were removed if present due to missing files (no copying occurred).");
		}
	else
		{
		print "\tNo series directories were removed due to missing files.\n";
		Logit("No series directories were removed due to missing files.");
		}
	if ($processed == 0)
		{
		print "\tNo series were processed.\n\n";
		Logit("No series were processed.");
		}
	else
		{
		print "\t$processed series were processed.\n\n";
		Logit("$processed series were processed.");
		}
	if ($processed != 0)
		{
		print "\tThe script ran for approximately $totaltime seconds.\n";# and averaged $averagemovetime seconds per move.\n\n";
		Logit("The script ran for approximately $totaltime seconds.");
		}
	else
		{
		print "\tThe script did not move any data, so no statistics can be provided.\n\n";
		Logit("The script did not move any data, so statistics cannot be provided.");
		}
	chomp(my $currentdir = `pwd`);
	print "\nUVPMT log located in: /opt/emageon/manager/rs-tools/log/uvpmt_activity_log_$timestamp.txt\n\n";
    }
############################################################################################
#
# Commandline parameter printout
#
############################################################################################

sub parameter_printout
    {
    print "The follow options are available for commanline execution:\n\n";
    print "\t-s or --sourceid <dbid>          Radsuite DB source volume ID.\n";
    print "\t-r or --destinationid <dbid>     Radsuite DB destination volume ID.\n\n";
    print "Other supplemental options to be used with parameters above\n\n";
    print "\t-q or --quantity <number>        Quantity of series to move from source to destination.\n";
    print "\t-n or --no-confirm               Do not prompt used to proceed with moving forward after all values are provided.\n";
    print "\t-x or --no-cleanupdirs           Do not copy then remove the directories for the unidentified files in source partition.\n";
    print "\t-d or --debug                    provides more details in general log.\n";
    print "\t-h or --help                     Prints this help menu and all other arguments are ignored.\n";
    print "\n";
    print "The following paramater formats are suppported (\"--debug\" or \"-d\" can be added as a parameter at any time):\n\n";
    print "script -d -n -x                                                 (Directs you to a menu,debug logs, will not prompt after selection is made, will not cleanup remaining directories)\n";
    print "script --debug --source <dbid> --destination <dbid>             (Moves all series from Source id to Destination id with debug logs)\n";
    print "script -s <dbid> -r <dbid> -q <quantity>                        (Moves X series from Source id to Destination id with default logs)\n";  
    print "script -s <dbid> -r <dbid> -q <quantity> -d                     (Moves X series from Source id to Destination id with debug logs)\n";
    print "script -s <dbid> -r <dbid> -q <quantity> -n -x                  (Moves X series from Source id to Destination id with no confirmation after selections are made and no directory cleanup)\n\n";
	print "I.E.:  ./uv_partition_migration_tool.pl -s 1 -r 2 -q 2500 -d    (Moves 2500 series from Source id 1 to Destination id 2 with debug logs)\n";
	
	exit;
    }

############################################################################################
#
# Check for existing process
#
############################################################################################

sub check_for_existing_process
	{
	$pidfile = "/var/run/uv_partition_migration_tool.pid";
	if (-e $pidfile)
		{
		print "Process already running or stale pid file exists $pidfile\n";
		exit;
		}
	else
		{
		system("echo $$ > $pidfile");
		}
	}
	
############################################################################################
#
# Log the events
#
############################################################################################

sub Logit {
  $logtimestamp=`date +%Y%m%d_%H%M%S`;
  chomp($logtimestamp);
  open (LOG, ">> $log") or die "LOG: $!\n";
  my ($what) = @_;
  $what = $logtimestamp." ".$what;
  print LOG "$what\n";
  close LOG;
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

# Scram
sub Scram {
  Close_Database();
  Logit ('Script was killed');
  clean_exit();
}

# Scram
sub clean_exit {
  Logit ('Done');
  summary();
  unlink($pidfile);
  exit;
}