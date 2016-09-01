#!/usr/bin/perl -w

use strict;
use DBI;
use Scalar::Util qw(looks_like_number);
use Getopt::Long;
use File::stat;
#use Time::localtime;
use Time::Piece;

# Set the signal interrups
$SIG{INT} = \&Scram;     # CTRL-C, kill -2
$SIG{QUIT} = \&Scram;    # CTRL-D, kill -3
$SIG{TERM} = \&Scram;    # kill or kill -15

# Program variables

use vars qw(
			$pidfile
			$noconfirm
			$fn
    		$fs
    		$isMultiTieredNFS
    		$isMultiTiered
    		$MultiTieredNFS_basedir
    		$isConcatenatedDate
			$basedir
			$datefilter
			$logtimestamp
			$timestamp
			$parameter
			$loglevel
			$status
			$log
			$non_dcm
			$failed_store_log
			$query_log
			$sop
			$seuid
			$stuid
			$mod
			$args
			$dbh
			@dirs
			$sop_sth
			$img_sth
			$sr_sth
			$snr_sth
			$AU_sth
			$PR_sth
			$KO_sth
			$sop_sth_qr
			$img_sth_qr
			$sr_sth_qr
			$snr_sth_qr
			$AU_sth_qr
			$PR_sth_qr
			$KO_sth_qr
			$java
			$dcp_success
			$sae
			$ae
			$host
			$port
			$QCae
			$QChost
			$QCport
			$isFIC
			$isSourceFile
			$isQuery
			$sourcefile
			$counter
			@mods
			$trace
			$getoptresults
			$help
			$parmmods
			);
$args=0;
$timestamp = `date +%Y%m%d_%H%M%S`;
chomp($timestamp);
$loglevel = 0;
$ENV{JAVA_HOME} = "/usr/java";
$datefilter = 0;

eval 
	{
	my $value;
	$getoptresults = GetOptions ( 
			'debug|d' 			=> \$loglevel,
			'trace|t'			=> \$trace,
			'help|h' 			=> \$help,
			'no-confirm|n'		=> \$noconfirm,
			'query|q'			=> \$isQuery,
			'f|r|RIC|FIC'		=> \$isFIC,
			'uvdir|u=s'			=> \$basedir,
			'filepath|p=s'		=> \$sourcefile,
			'date-filter=i'		=> \$datefilter,
			'sae=s'				=> \$sae,
			'ae=s'				=> \$ae,
			'host=s'			=> \$host,
			'port=i'			=> \$port,
			'qcae=s'			=> \$QCae,
			'qchost=s'			=> \$QChost,
			'qcport=i'			=> \$QCport,
			'send=s'			=> \$parmmods
			);
	if($parmmods)
		{
		$parmmods = uc($parmmods);
		@mods = split /,/, $parmmods;
		}		
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
	if ($loglevel)	{print "DEBUG        = $loglevel\n";}
	if ($trace)		{print "TRACE        = $trace\n";}
	if ($parmmods)	{print "SEND         = $parmmods\n";}
	if ($isQuery)	{print "QUERY TRUE   = $isQuery\n";}
	if ($isFIC)		{print "FIC|RIC TRUE = $isFIC\n";}	
	if ($noconfirm)	{print "NO-CONFIRM   = $noconfirm\n";}
	if ($basedir)	{print "UVDIR        = $basedir\n";}
	if ($sourcefile){print "FILEPATH     = $sourcefile\n";}
	if ($datefilter){print "DATE_FILTER  = $datefilter\n";}
	if ($sae)		{print "SAE          = $sae\n";}
	if ($ae)		{print "AE           = $ae\n";}
	if ($host)		{print "HOST         = $host\n";}
	if ($port)		{print "PORT         = $port\n";}
	if ($QCae)		{print "QCAE         = $QCae\n";}
	if ($QChost)	{print "QCHOST       = $QChost\n";}
	if ($QCport)	{print "QCPORT       = $QCport\n";}
	}

check_for_help();
check_for_existing_process();
get_log_level();
get_parms();
validate_destination();
validate_QCdestination();
get_storage_properties();
get_dirs();
prepare_db();
process_files();

sub get_parms
	{
	if ($noconfirm)
        {
        print "Application set to automatically proceed without prompting\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Application set to automatically proceed without prompting.");
			}
        }
        
	if ($isQuery)
        {
        print "Application set to query only.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Application set to query only.");
			}
        }
	
	if ($isFIC)
        {
        print "Application set to query the proxy and local EA DB for results.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Application set to query the Destination/Proxy AE and local EA DB for results.");
			}
        }
    else
    	{
    	print "Application set to query the local EA DB for results.\n";	
    	if ($loglevel)
            	{
				Logit("DEBUG: Application set to query the local EA DB for results.");
				}	
    	}
	if ($basedir)
        {
        print "UV Base directory set to $basedir.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: UV Base directory set to $basedir.");
			}
        }
		
	if ($sourcefile)
        {
        print "Filepath set to $sourcefile.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Filepath set to $sourcefile.");
			}
        }


	if ($datefilter)
        {
        print "Date Filter set to $datefilter.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Date Filter set to $datefilter.");
			}
        }

	if ($sae)
        {
        print "Source AE set to $sae.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Source AE set to $sae.");
			}
        }

	if ($ae)
        {
        print "Destination/Proxy AE set to $ae.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Destination/Proxy AE set to $ae.");
			}
        }

	if ($port)
        {
        print "Destination/Proxy host set to $host.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Destination/Proxy host set to $host.");
			}
        }


	if ($port)
        {
        print "Destination/Proxy port set to $port.\n";
        if ($loglevel)
          	{
			Logit("DEBUG: Destination/Proxy port set to $port.");
			}
        }
	if ($QCae)
        {
        print "QC Destination AE set to $QCae.\n";
		if ($loglevel)
           	{
			Logit("DEBUG: QC Destination AE set to $QCae.");
			}
        }

	if ($QChost)
       	{
        print "QC Destination host set to $QChost.\n";
		if ($loglevel)
        	{
			Logit("DEBUG: QC Destination host set to $QChost.");
			}
		}	
	if ($QCport)
		{
        print "QC Destination port set to $QCport.\n";
        if ($loglevel)
        	{
			Logit("DEBUG: QC Destination port set to $QCport.");
			}
		}
	
	if (@mods)
		{
		my $line = "Application set to send ";
        foreach my $item (@mods)
          	{
           	$line .= "$item,";
           	if ($loglevel == 1)
           		{
           		Logit("DEBUG: Application set to send $item files.");
				}
           	}
        chop($line);
        $line .= " modality files\n";
        print $line;
		}
	
	my $isValidParms;	
	#check for valid LTA query mode parameters
	if (($isQuery) and (! $QCae) and (! $isFIC) and (! $ae) and (! $sae) and (! $isSourceFile) and (! @mods) and (! $datefilter)) 
		{
		$isValidParms = 1;	
		}
	#check for valid FIC/RIC query mode parameters
	elsif (($isQuery) and ($isFIC) and (! $QCae) and ($ae) and ($host) and ($port) and (! $isSourceFile) and (! @mods) and (! $datefilter))
		{
		$isValidParms = 1;	
		}	 
	#check for valid LTA action mode parameters
	elsif ((! $isQuery) and (! $isFIC) and ($sae) and ($ae) and ($host) and ($port) and ( $isSourceFile ) and (! $basedir))
		{
		$isValidParms = 1;	
		}
	elsif ((! $isQuery) and (! $isFIC) and ($sae) and ($QCae) and ($QChost) and ($QCport) and ( $isSourceFile ) and (! $basedir))
		{
		$isValidParms = 1;	
		}

	elsif ((! $isQuery) and (! $isFIC) and ($sae) and ($ae) and ($host) and ($port) and (! $isSourceFile ) and ($basedir))
		{
		$isValidParms = 1;	
		}

 	elsif ((! $isQuery) and (! $isFIC) and ($sae) and ($QCae) and ($QChost) and ($QCport) and (! $isSourceFile ) and ($basedir))
		{
		$isValidParms = 1;	
		}
 
	#check for valid FIC/RIC action mode parameters
	elsif ((! $isQuery) and ($isFIC) and ($sae) and ($ae) and ($host) and ($port) and (! $QCae) and (! $QChost) and (! $QCport) and ( $isSourceFile ) and (! $basedir))
		{
		$isValidParms = 1;	
		}
	elsif ((! $isQuery) and ($isFIC) and ($sae) and ($ae) and ($host) and ($port) and ($QCae) and ($QChost) and ($QCport) and ( $isSourceFile ) and (! $basedir))
		{
		$isValidParms = 1;	
		}

	elsif ((! $isQuery) and ($isFIC) and ($sae) and ($ae) and ($host) and ($port) and (! $QCae) and (! $QChost) and (! $QCport) and (! $isSourceFile ) and ($basedir))
		{
		$isValidParms = 1;	
		}

	elsif ((!$isQuery) and ($isFIC) and ($sae) and ($ae) and ($host) and ($port) and ($QCae) and ($QChost) and ($QCport) and (! $isSourceFile ) and ($basedir))
		{
		$isValidParms = 1;	
		}
    
	if (! $isValidParms or ((! $isSourceFile ) and (! $basedir)) )
		{
        unlink($pidfile);
		parameter_printout();
		}
		
	}

sub validate_destination
	{
	if ($ae)
		{
		$dcp_success = 0;
		my $PWD=`pwd`;
		chomp($PWD);
		my @results = `cd /opt/emageon/manager/rs-tools/dcm4che/bin/; ./dcmecho -L $sae $ae\@$host:$port; cd $PWD`;
		foreach my $line (@results)
			{
			if ($line =~ /Perform Verification/)
				{
				$dcp_success = 1;
				if ($loglevel == 1)
        	    	{
       		     	Logit ("DEBUG: $line");
        	    	}
				}
			}
		foreach my $line (@results)
			{
			if ($line =~ /Failed to establish association/)
				{
				$dcp_success = 0;
				if ($loglevel == 1)
            		{
            		Logit ("DEBUG: $line");
            		}
				}		
			}
		if ($dcp_success == 0)
			{
			print "Unable to connect to dicom echo destination/proxy: $ae $host:$port\n";
			clean_exit();
			}
		}
	}
sub validate_QCdestination
	{
	if ($QCae)
		{
		$dcp_success = 0;
		my $PWD=`pwd`;
		chomp($PWD);
		my @results = `cd /opt/emageon/manager/rs-tools/dcm4che/bin/; ./dcmecho -L $sae $QCae\@$QChost:$QCport; cd $PWD`;
		foreach my $line (@results)
			{
			if ($line =~ /Perform Verification/)
				{
				$dcp_success = 1;
				if ($loglevel == 1)
        	    	{
       		     	Logit ("DEBUG: $line");
        	    	}
				}
			}
		foreach my $line (@results)
			{
			if ($line =~ /Failed to establish association/)
				{
				$dcp_success = 0;
				if ($loglevel == 1)
            		{
            		Logit ("DEBUG: $line");
            		}
				}		
			}
		if ($dcp_success == 0)
			{
			print "Unable to connect to dicom echo QC destination: $QCae $QChost:$QCport\n";
			clean_exit();
			}
		}
	}

sub get_dirs
	{
	if($basedir)
		{
		# Get a list of directories
		@dirs = `find $basedir -maxdepth 1 -type d | sort`;
		if ($loglevel == 1)
           	{
           	Logit ("DEBUG: The following directories were found:");
           	foreach my $line (@dirs)
           		{
				chomp($line);
           		Logit ("DEBUG: $line");
           		}
           	}
		}
	}
sub prepare_db
	{
	# Open the log (and disable buffering)
	
	eval
		{
		open(LOG, ">> $log");
		select((select(LOG), $| = 1)[0]);
		close(LOG);
		};
	if($@)
		{
		Scram($_);
		}
	eval
		{
		open(LOG, ">> $non_dcm");
		select((select(LOG), $| = 1)[0]);
		close(LOG);
		};
	if($@)
		{
		Scram($_);
		}

	# Connect to the db
	Open_Database('UV_DICOM', 'UV_DICOM');

	$img_sth_qr = "select count(*) from image where sopinstanceuid = ?";
	$img_sth = $dbh->prepare($img_sth_qr) or die "Can't prepare 'img_sth': " . $DBI::errstr . "\n";
	
	$sr_sth_qr = "select count(*) from structuredreport where studyinstanceuid = ?";
	$sr_sth = $dbh->prepare($sr_sth_qr) or die "Can't prepare 'sr_sth': " . $DBI::errstr . "\n";
	
	$snr_sth_qr = "select count(*) from studynotereport where refstudyinstanceuid = ?";
	$snr_sth = $dbh->prepare($snr_sth_qr) or die "Can't prepare 'snr_sth': " . $DBI::errstr . "\n";
	
	$AU_sth_qr = "select count(*) from waveform where sopinstanceuid = ?";
	$AU_sth = $dbh->prepare($AU_sth_qr) or die "Can't prepare 'AU_sth': " . $DBI::errstr . "\n";
	
	$PR_sth_qr = "select count(*) from presentationstate where sopinstanceuid = ?";
	$PR_sth = $dbh->prepare($PR_sth_qr) or die "Can't prepare 'PR_sth': " . $DBI::errstr . "\n";

	$KO_sth_qr = "select count(*) from keyobjectnote where sopinstanceuid = ?";	
	$KO_sth = $dbh->prepare($KO_sth_qr) or die "Can't prepare 'KO_sth': " . $DBI::errstr . "\n";
	}

sub process_files
	{
	# Go into the directories
	if (! $noconfirm)
		{
		print "Please press any key to continue or Ctrl^C to exit.\n";
		<STDIN>;
		}
	if (!@dirs)
		{
		push(@dirs, ' ');	
		}
	foreach my $dir ( @dirs ) 
		{
		my @files;
		my $fh;
		if ($dir eq ' ')
			{
			open(my $fh, '<', $sourcefile);
			while (my $row = <$fh>)
				{
				chomp($row);
				if (($row =~ /EANF/) and ($row =~ /AVNF/))
					{
					my @f = split /:/, $row;
					push(@files, $f[0]); 	
					}	
				}
			}
		else
			{
			chomp($dir);
			next if ($dir eq '.');
			chdir $dir;
			@files = `find $dir -type f`;
			}
		
		my $dirct = scalar(@files);
		foreach my $fil ( @files ) 
			{
			my $failed;	
			my $dcmfilterdate;
			my $isDICOM;
			chomp ($fil);
			my $fileDate = localtime((stat $fil)[9])->ymd('');
			my $fileTime = localtime((stat $fil)[9])->hms;
			my $lastmodified = $fileDate." ".$fileTime;
			my $filelength = -s $fil;
			if ($fil !~ /dcm$/)
				{
				Log_non_dcm("$fil");
				if ($loglevel == 1)
					{
					Logit ("DEBUG: Non Dicom File found: $fil");
					}
				$failed = 1;
				}
			elsif ($filelength == 0)
				{
				Log_non_dcm("$fil");
				if ($loglevel == 1)
					{
					Logit ("DEBUG: Empty or incomplete File found: $fil");
					}
				$failed = 1;
				}
			else
				{
				my $results = `file $fil`;
				#print $results;
				if ($results !~ /DICOM/)
					{
					Log_non_dcm("$fil");
					if ($loglevel == 1)
						{
						Logit ("DEBUG: Non DICOM file found: $fil");
						}
					$failed = 1;
					}
				
				}
			next if ($failed);
			if ($loglevel == 1)
      			{
       			Logit ("DEBUG: Dicom File found: $fil");
   				}			
			my ($studyuid, $seriesuid, $SOPuid, $studydate, $seriesdate, $mod, $desc, $snr, $sr, $img, $au, $pr, $ko, $AV, $ea_file);
			if ($loglevel == 1)
      			{
       			Logit ("DEBUG: Reading Dicom Header of $fil");
   				}
			my (@dcm) = `/usr/local/dicom/bin/dcmdump +p +P 0008,0020 +p +P 0008,0021 +p +P 0008,0060 +p +P 0008,103e +p +P 0008,0018 +p +P 0020,000e +p +P 0020,000d $fil `;
			foreach my $lin (@dcm) 
				{
				chomp ($lin);

				if ($lin =~ /^\(0008,0020\)/) 
					{
					if (!$studydate)
						{
						($studydate = $lin) =~ s/.*\[([^\]]+)\].*/$1/g;
						if ($loglevel == 1)
            				{
            				Logit ("DEBUG: Study date: $studydate");
            				}
						}
					}	
								
				if ($lin =~ /^\(0008,0021\)/) 
					{
					if (!$seriesdate)
						{
						($seriesdate = $lin) =~ s/.*\[([^\]]+)\].*/$1/g;
						if ($loglevel == 1)
            				{
            				Logit ("DEBUG: Series date: $seriesdate");
            				}
						}
					}				
				
				if ($lin =~ /^\(0008,0060\)/) 
					{
					if (!$mod)
						{
						($mod = $lin) =~ s/.*\[([^\]]+)\].*/$1/g;
						if ($loglevel == 1)
            				{
            				Logit ("DEBUG: Series Modality: $mod");
            				}
						}
					}
				elsif ($lin =~ /^\(0020,000d\)/) 
					{
					if (!$studyuid)
						{
						($studyuid = $lin) =~ s/.*\[([^\]]+)\].*/$1/g;
						if ($loglevel == 1)
            				{
            				Logit ("DEBUG: Study Instance UID: $studyuid");
            				}
						}
					}
				elsif ($lin =~ /^\(0008,103e\)/) 
					{
					if (!$desc)
						{
						($desc = $lin) =~ s/.*\[([^\]]+)\].*/$1/g;
						if ($loglevel == 1)
            				{
            				Logit ("DEBUG: Series Description: $desc");
            				}						
						}
					}
				elsif ($lin =~ /^\(0020,000e\)/)
					{
					if (!$seriesuid)
						{
						($seriesuid = $lin) =~ s/.*\[([^\]]+)\].*/$1/g;
						if ($loglevel == 1)
            				{
            				Logit ("DEBUG: Series Instance UID: $seriesuid");
            				}
						}
					}
				elsif ($lin =~ /^\(0008,0018\)/) 
					{
					if (!$SOPuid)
						{
						($SOPuid = $lin) =~ s/.*\[([^\]]+)\].*/$1/g;
						if ($loglevel == 1)
            				{
            				Logit ("DEBUG: SOP Instance UID: $SOPuid");
            				}						
						}
					}
				}

			if ( looks_like_number($seriesdate) and (length($seriesdate) == 8) )
				{
				$dcmfilterdate = $seriesdate;
				}
			elsif ( looks_like_number($studydate) and (length($studydate) == 8 ) )
				{
				$dcmfilterdate = $studydate;	
				}
			else
				{
				undef $dcmfilterdate;
				}
			
			my @fields = split /\//, $fil;
			my ($key, $stuid, $seuid, $filename) = splice(@fields, -4, 4);
			$sop = substr($filename, 0, length($filename) - 4);
			my $EA;
			my $storeddate = '';
			$ea_file = get_ea_storage_location();
			#print "EA File: $ea_file\n";

			if ($ea_file)
				{
				my $file;
				if ((-e $ea_file) and ($ea_file =~ /dicom/))
					{
					($file, undef) = split /-/, $ea_file;
					my $fileDate = localtime((stat $file)[9])->ymd('');
					my $fileTime = localtime((stat $file)[9])->hms;
					$storeddate = $fileDate." ".$fileTime;
					}
				if ($loglevel)
            		{
					Logit("DEBUG: Sop instance uid found in EA filesystem at: $ea_file");
					}
				$EA = 1;	
				}			
			if (($isFIC) and (!$ea_file))
				{
				my $PWD=`pwd`;
				my $results;
				chomp($PWD);
				my $fileDate;
				my $fileTime;
				my @queryresults = `cd /opt/emageon/manager/rs-tools/dcm4che/bin/; ./dcmqr -q00080018=$sop -r00080018 -r00080020 -r00080030 -L $sae $ae\@$host:$port; cd $PWD`;
				foreach my $result (@queryresults)
					{
					chomp($result);
					if ($result =~ /Received 1 matching entries/)
						{
						$EA = 1;
						#print "EA query found file true.\n";
						if ($loglevel == 1)
            				{
							Logit("DEBUG: Sop instance uid $sop not found on local EA DB, but found on LTA.")
							}
						}
					if (!$storeddate)
						{
						if ($result =~ /^\(0008,0020\).*\#8/)
							{
							($fileDate = $results) =~ s/.*\[([^\]]+)\].*/$1/g;
							}
						elsif ($result =~ /^\(0008,0030\).*\#10/)
							{
							($fileTime = $results) =~ s/.*\[([^\]]+)\].*/$1/g;
							}
						}
					last if ($storeddate);
					}
				if ($EA)
					{
					if (!$fileDate){$fileDate = "00000000";}	
					if (!$fileTime){$fileTime = "00:00:00";}
					$storeddate = $fileDate." ".$fileTime;
					}
				}
			$EA = $EA ? 'EA' : 'EANF';
			if ($mod eq 'SR') 
				{
				if ($desc && $desc =~ /Emageon/) 
					{
					if ($loglevel == 1)
							{
            				my $query = $snr_sth_qr;
            				my $find = '\?';
            				my $replace = "'$stuid'";
            				$query =~ s/$find/$replace/;
            				Logit("DEBUG: Executing query: $query");
            				}
					$snr_sth->execute($stuid);
					$snr = $snr_sth->fetchrow_array;
					}
				else 
					{
					if ($loglevel == 1)
            				{
            				my $query = $sr_sth_qr;
            				my $find = '\?';
            				my $replace = "'$stuid'";
            				$query =~ s/$find/$replace/;
            				Logit("DEBUG: Executing query: $query");
            				}
					$sr_sth->execute($stuid);
					$sr = $sr_sth->fetchrow_array;
					}
				}
			else 
				{
				if ($mod eq 'AU') 
					{
					if ($loglevel == 1)
            				{
            				my $query = $AU_sth_qr;
            				my $find = '\?';
            				my $replace = "'$sop'";
            				$query =~ s/$find/$replace/;
            				Logit("DEBUG: Executing query: $query");
            				}
					$AU_sth->execute($sop);
					$au = $AU_sth->fetchrow_array;
					}
				elsif ($mod eq 'KO') 
					{
					if ($loglevel == 1)
            				{
            				my $query = $KO_sth_qr;
            				my $find = '\?';
            				my $replace = "'$sop'";
            				$query =~ s/$find/$replace/;
            				Logit("DEBUG: Executing query: $query");
            				}
					$KO_sth->execute($sop);
					$ko = $KO_sth->fetchrow_array;
					}
				elsif ($mod eq 'PR') 
					{
					if ($loglevel == 1)
            				{
            				my $query = $PR_sth_qr;
            				my $find = '\?';
            				my $replace = "'$sop'";
            				$query =~ s/$find/$replace/;
            				Logit("DEBUG: Executing query: $query");
            				}
					$PR_sth->execute($sop);
					$pr = $PR_sth->fetchrow_array;
					}
				else 
					{
					if ($loglevel == 1)
            				{
            				my $query = $img_sth_qr;
            				my $find = '\?';
            				my $replace = "'$sop'";
            				$query =~ s/$find/$replace/;
            				Logit("DEBUG: Executing query: $query");
            				}
					$img_sth->execute($sop);
					$img = $img_sth->fetchrow_array;
					}
				}
			
			$AV = $sr || $snr || $img || $au || $ko || $pr ? 'AV' : 'AVNF';
			$img = $img ? 'IMG' : '-';
			$sr = $sr ? 'SR' : '-';
			$snr = $snr ? 'SNR' : '-';
			$au = $au ? 'AU' : '-';
			$ko = $ko ? 'KO' : '-';
			$pr = $pr ? 'PR' : '-';
			$storeddate= $storeddate ? $storeddate : '-';
			$lastmodified = $lastmodified ? $lastmodified : '-';
			if ($isQuery)
				{
				Log_query ("$fil|$lastmodified|$AV|$EA|$storeddate|$mod|$img|$sr|$snr|$au|$ko|$pr");	
				}
			elsif ((!$dcmfilterdate ) or ($dcmfilterdate > $datefilter) and (!$isQuery) )
				{			
				if (($AV eq "AV") and ($EA eq "EA"))
					{
					if ($loglevel == 1)
            			{
            			Logit ("DEBUG: INFO 1 - Found in Archive and in AV, nothing to do for SOP uid: $sop");
            			}
					}    
				elsif (($EA eq "EA") and ($AV eq "AVNF"))
					{
					if ($loglevel == 1)
            			{
						Logit ("DEBUG: INFO 2 - Found in Archive and not in AV, nothing to do for SOP uid: $sop");
						}
					}
				elsif (($EA eq "EANF") and ($AV eq "AV"))
					{
					if ($loglevel == 1)
            			{
						Logit ("DEBUG: INFO 3 - Found in AV and not in Archive, nothing to do for SOP uid: $sop");
						}
					}
				elsif (($EA eq "EANF") and ($AV eq "AVNF"))
					{
					my $PWD=`pwd`;
					chomp($PWD);
					my @results;
					if (!$isQuery)
						{
						if (!$QCae)
							{
							if (@mods)
								{
								foreach my $type (@mods)
									{
									if ($type eq $mod)
										{
										Logit ("ERROR - Not found anywhere, attempting to send sopinstanceuid $sop to $ae using dcm4che.");	 
										@results = `cd /opt/emageon/manager/rs-tools/dcm4che/bin/; ./dcmsnd -L $sae $ae\@$host:$port $fil 2>&1; cd $PWD`;
										$sop_sth->execute($sop);
										my $EA = $sop_sth->fetchrow_array;
										if ($EA)
											{
											Logit ("\tSuccessfully stored sopinstanceuid: $sop to EA");
											}
										else
											{
											Logit ("\tFailed to store sopinstanceuid: $sop to EA");
											foreach my $line (@results)
												{
												chomp($line);
												if ($line =~ /error=/)
													{
													Logit ("\t$line");
													}
												}
											Log_failed_store($fil);					
											}									
										}
									else
										{
										Logit ("ERROR - Not found anywhere, but modality is not included is list of modality types to send, ignoring.");	
										}
									}
								}
							else
								{
								Logit ("ERROR - Not found anywhere, attempting to send sopinstanceuid $sop to $ae using dcm4che.");
								@results = `cd /opt/emageon/manager/rs-tools/dcm4che/bin/; ./dcmsnd -L $sae $ae\@$host:$port $fil 2>&1; cd $PWD`;
								$sop_sth->execute($sop);
								my $EA = $sop_sth->fetchrow_array;
								if ($EA)
									{
									Logit ("\tSuccessfully stored sopinstanceuid: $sop to EA");
									}
								else
									{
									Logit ("\tFailed to store sopinstanceuid: $sop to EA");
									foreach my $line (@results)
										{
										chomp($line);
										if ($line =~ /error=/)
											{
											Logit ("\t$line");
											}
										}
									Log_failed_store($fil);					
									}
								}			
							}
						else
							{
							if (@mods)
								{
								foreach my $type (@mods)
									{
									if ($type eq $mod)
										{
										Logit ("ERROR - Not found anywhere, attempting to send sopinstanceuid $sop to $QCae using dcm4che.");	 
										@results = `cd /opt/emageon/manager/rs-tools/dcm4che/bin/; ./dcmsnd -L $sae $QCae\@$QChost:$QCport $fil 2>&1; cd $PWD`;
										foreach my $line (@results)
												{
												chomp($line);
												if ($line =~ /error=/)
													{
													Logit ("\t$line");
													Log_failed_store($fil);
													}
												}
																		
										}
									else
										{
										Logit ("ERROR 2 - Not found anywhere, but modality is not included is list of modality types to send, ignoring.");	
										}
									}
								}
							else
								{
								Logit ("ERROR 1 - Not found anywhere, attempting to send sopinstanceuid $sop to $QCae using dcm4che.");	 
								@results = `cd /opt/emageon/manager/rs-tools/dcm4che/bin/; ./dcmsnd -L $sae $QCae\@$QChost:$QCport $fil 2>&1; cd $PWD`;
								foreach my $line (@results)
									{
									chomp($line);
									if ($line =~ /error=/)
										{
										Logit ("\t$line");
										Log_failed_store($fil);
										}
									}
								}			
							}
						}
					}
				}
			else
				{
				if ($loglevel == 1)
					{
					Logit ("DEBUG: Date Filtered: $dcmfilterdate skipped file: $fil");
					}	
				}
			print ".";
			if ($loglevel == 1)
				{
				Logit ("DEBUG: End of file check for sop instanceuid --- $sop");
				}	
				
			}
		if ($dirs[0] ne " " )
			{	
			chdir $basedir;
			}
		}
	# Close files
	print "\n\nDone!\n";
	clean_exit();
	}

############################################################################################
#
# Logit
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
# Logit Non Dicom Files
#
############################################################################################
sub Log_non_dcm {
  open (LOG, ">> $non_dcm") or die "LOG: $!\n";
  my ($what) = @_;
  print LOG "$what\n";
  close LOG;
}

############################################################################################
#
# Logit Failed Dicom File
#
############################################################################################
sub Log_failed_store {
  open (LOG, ">> $failed_store_log") or die "LOG: $!\n";
  my ($what) = @_;
  print LOG "$what\n";
  close LOG;
}

############################################################################################
#
# Logit Query Results File
#
############################################################################################
sub Log_query {
  open (LOG, ">> $query_log") or die "LOG: $!\n";
  my ($what) = @_;
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
		}
	else
		{
		Logit ('Script was killed');
		$sop_sth->finish;
		}
	Close_Database();
	clean_exit();
}

############################################################################################
#
# Clean Exit
#
############################################################################################

sub clean_exit 
	{
	unlink($pidfile);
	print "Log file location: $log\n";
	if ( -e $non_dcm )
		{
		print "Non Dicom file list location: $non_dcm\n";
		}
	if ( -e $failed_store_log )
		{
		print "Failed Dicom Store log location: $failed_store_log\n";
		}
	if ( -e $query_log )
		{
		print "Query Results log location: $failed_store_log\n";
		}
	exit;
	}


############################################################################################
#
# Check for existing process
#
############################################################################################

sub check_for_existing_process
	{
	$pidfile = "/var/run/radsuite_fs_crawler.pid";
	if (-e $pidfile)
		{
		print "Process already running or stale pid file exists $pidfile\n";
		exit;
		}
	else
		{
		system("echo $$ > $pidfile")
		}
	}


############################################################################################
#
# Parameter Printout
#
############################################################################################

sub parameter_printout
	{
	print "Below are valid examples of script parameter combinations:\n\n";
	
	print "Additional general option parameters.\n\n";
	print "options not shown in examples       example syntax         Description\n";
	print "------------------------------------------------------------------------------------------------------------------------------------------------\n";
	print "-help or -h                         -help                  Provides this output screen and cannot be used with any other paramater\n";
	print "-debug or -d                        -debug                 Provides more detailed in the general log\n\n";
		
	print "These are the only valid parameter sets Like the options listed above anything wrap in parenthesis () is optional in given examples below.\n";
	
	print "\nQuery mode parameters:\n\n";

	print "Interrogate directory, read db, generate query results file\n";
	print "\t$0 -uvdir <directory> -LTA -query\n";

	print "Interrogate directory, read db, query from Proxy AE title, generate query results file\n";
	print "\t$0 -uvdir <directory> -FIC -query -sae <Source AE> -ae <dest. AE> -host <dest. host> -port <dest. port>\n";	
	print "------------------------------------------------------------------------------------------------------------------------------------------------\n";
	print "\nAdditional optional action mode parameters.\n\n";
	print "options not shown in examples       example syntax         Description\n";
	print "------------------------------------------------------------------------------------------------------------------------------------------------\n";
	print "-date-filter <YYYYMMDD>             -date-filter 20140105  Filters all actions and queries to files with a series or study date newer than the date provided\n";
	print "-send <comma separated list>        -send MG,XA,US,SR,SC   Filters all actions only to files of modality type indicated in dicom header\n\n";


	print "\nAction mode parameters:\n\n";

	print "Read from file, send to proxy AE title\n";
	print "\t$0 -filepath <input file> -LTA -sae <Source AE> -ae <dest. AE> -host <dest. host> -port <dest. port>\n";

	print "Read from file, send to QC AE title\n";
	print "\t$0 -filepath <input file> -LTA -sae <Source AE> -qcae <QC AE> -qchost <qc host> -qcport <qc port>\n";
	
	print "Interrogate directory, send to proxy AE title\n";
	print "\t$0 -uvdir <directory> -LTA -sae <Source AE> -ae <dest. AE> -host <dest. host> -port <dest. port>\n";
	
	print "Interrogate directory, send to QC AE title\n";
	print "\t$0 -uvdir <directory> -LTA -sae <Source AE> (-qcae <QC AE> -qchost <qc host> -qcport <qc port>)\n";
	
	print "Read from file, query and send to proxy AE title\n";
	print "\t$0 -filepath <input file> -FIC -sae <Source AE> -ae <dest. AE> -host <dest. host> -port <dest. port>\n";
	
	print "Read from file, query proxy AE ( and send to QC AE)\n";
	print "\t$0 -filepath <input file> -FIC -sae <Source AE> -ae <dest. AE> -host <dest. host> -port <dest. port> (-qcae <QC AE> -qchost <qc host> -qcport <qc port>)\n";
	
	print "Interrogate directory, query and send to proxy AE title\n";
	print "\t$0 -uvdir <directory> -FIC -sae <Source AE> -ae <dest. AE> -host <dest. host> -port <dest. port>\n";
	
	print "Interrogate directory, query proxy AE ( and send to QC AE title)\n";
	print "\t$0 -uvdir <directory> -FIC -sae <Source AE> -ae <dest. AE> -host <dest. host> -port <dest. port> (-qcae <QC AE> -qchost <qc host> -qcport <qc port>)\n\n";
	
	print "Also ensure you are using a valid uv directory or filepath in your script. Possible uv directories are below:\n\n";
	system('echo "select prefix base_directory from image_volumes;" | sqlplus -S UV_DICOM/UV_DICOM');

	exit;
	}

###########################################################################
#
# Check for Help flag
#
###########################################################################

sub check_for_help
        {
        foreach $parameter (@ARGV)
                {
                $args++;
                if ( ($parameter eq "-h") or ($parameter eq "--help") or (!@ARGV) or ($parameter eq "-help"))
                        {
						parameter_printout();
						exit;
                        }
                }
        }

###########################################################################
#
# Check Logging level
#
#
###########################################################################

sub get_log_level
    {
    
	if (!(-e '/opt/emageon/manager/rs-tools/log' and -d '/opt/emageon/manager/rs-tools/log'))
        {
	    system ('mkdir /opt/emageon/manager/rs-tools/log');
       	}
	$failed_store_log = "/opt/emageon/manager/rs-tools/log/failed_dicom_store_$timestamp.log";
	$non_dcm = "/opt/emageon/manager/rs-tools/log/non_dcm_$timestamp.log";
	$log = "/opt/emageon/manager/rs-tools/log/av_crawl_$timestamp.log";
	$query_log = "/opt/emageon/manager/rs-tools/log/av_crawl_query_$timestamp.log";
    foreach $parameter (@ARGV)
        {
        if (($parameter eq "-d") or ($parameter eq "--debug"))
            {
            $loglevel = 1;
            #print "Logging level set to debug.\n";
			Logit("DEBUG: Logging level set to debug.");
            my $index = 0;;
            $index++ until ($ARGV[$index] eq '--debug') or ($ARGV[$index] eq '-d');
            splice(@ARGV, $index, 1);
            }
        }
    if ($loglevel == 0)
		{
		Logit("Logging level set to default");
        }
    }

sub get_storage_properties
	{
	my $prop_file = '/opt/emageon/manager/etc/emageon.properties';
    open( my $fh, '<', $prop_file ) or die "Can't open $prop_file: $!";
    while ( my $line = <$fh> )
		{
		if ( $line =~ /DataSetStorage\|FileNamingStrategy\|Class/ )
        	{
			chomp($line);
			my @emg_prop = split(/\./, $line);
			$fn = $emg_prop[-1];
			print "FileNamingStrategy = $fn\n";
			}
		if ( $line =~ /DataSetStorage\|FileNamingStrategy\|MRNSplitUnit/ )
			{
			chomp($line);
			my @emg_prop = split(/\=/, $line);
 			$fs = $emg_prop[-1];
			print "File directory split value = $fs\n";
			}
		if ( $line =~ /MultiTieredStorage\=MultiTieredStorage/ )
			{
			chomp($line);
			$isMultiTiered = 1;
			print "MultiTiered storage is present.\n";
			}
        if ( $line =~ /NFSTierStorage\|StorageDirectory/ )
			{
			chomp($line);
			my @emg_prop = split(/\=/, $line);
			$isMultiTieredNFS = 1;
			$MultiTieredNFS_basedir = $emg_prop[-1];
			print "MultiTiered storage is NFS and base directory structure is: $MultiTieredNFS_basedir\n";
			}
		if ( $line =~ /imageon\.archive\.ce\.storage\.nfstierstorage\.datasetstorage\.NFSTierFileNamingStrategy/ )
			{
			chomp($line);
			$isConcatenatedDate = 1;
			print "MultiTiered NFS storage used concatenated date format YYYYMMDD.\n";
			}
		}
		if((!$isConcatenatedDate) and ($isMultiTieredNFS))
			{
			print "MultiTiered NFS storage used split date format YYYY/(M)M/(D)D.\n";
			}
		if(($isMultiTiered) and (!$isMultiTieredNFS))
			{
			print "Non NFS MultiTiered storage is present.\n";
			}
	close $fh;
	}

sub get_ea_storage_location
	{
	my @path;
    my $filepath;
    my $dss;
    my $spid;
    my $sop_sth_qr;
    my $sop_sth;
    my $results;
        if (!$isMultiTiered)
                {

        $sop_sth_qr = "select distinct clarcdbo.patient.patientid,
                                        clarcdbo.study.studykey,
                                        clarcdbo.study.studyinstanceuid,
                                        clarcdbo.study.storagepartitionid from clarcdbo.patient
                                                join clarcdbo.study on clarcdbo.patient.patientkey=clarcdbo.study.patientkey
                                                join clarcdbo.series on clarcdbo.study.studykey=clarcdbo.series.studykey
                                                join clarcdbo.object on clarcdbo.series.serieskey=clarcdbo.object.serieskey
                                                where clarcdbo.object.sopinstanceuid = ?";
                }
        else
                {
        $sop_sth_qr = "select distinct clarcdbo.patient.patientid,
                                                clarcdbo.study.studykey,
                                                clarcdbo.study.studyinstanceuid,
                                                clarcdbo.study.storagepartitionid,
                                                clarcdbo.studylocation.storageid,
                                                to_char(clarcdbo.study.studydate, 'YYYYMMDD'),
                                                to_char(clarcdbo.study.studydate, 'YYYY'),
                                                to_char(clarcdbo.study.studydate, 'MM'),
                                                to_char(clarcdbo.study.studydate, 'DD')
                                                from clarcdbo.patient
                                                join clarcdbo.study on clarcdbo.patient.patientkey=clarcdbo.study.patientkey
                                                join clarcdbo.series on clarcdbo.study.studykey=clarcdbo.series.studykey
                                                join clarcdbo.object on clarcdbo.series.serieskey=clarcdbo.object.serieskey
                                                join clarcdbo.studylocation on clarcdbo.study.studykey=clarcdbo.studylocation.studykey
                                                where clarcdbo.object.sopinstanceuid = ?";
                }
        $sop_sth = $dbh->prepare($sop_sth_qr) or die "Can't prepare '$sop_sth_qr': " . $DBI::errstr . "\n";
        if ($loglevel == 1)
                {
                my $query = $sop_sth_qr;
                my $find = '\?';
                my $replace = "'$sop'";
                $query =~ s/$find/$replace/;
                #Logit ("DEBUG: Executing query: $query\n");
                }
        $sop_sth->execute($sop);
        while (my @row = $sop_sth->fetchrow_array())
                {
                if (($isMultiTiered)and (!$row[3]))
                        {
                        $spid = $row[4];
                        $spid =~ s/ESP//;
                        if ($spid<10) { $spid = "0".$spid; }
                        }
                else
                        {
                        $spid = $row[3];
                        $spid =~ s/SP//;
                        if ($spid<10) { $spid = "0".$spid; }
                        $dss = "dss".$spid;
                        }
                if ($fn =~ /StudyDBKeyFileNamingStrategy/)
                        {
                        @path = split //,$row[1];
                        }
                else
                        {
                        @path = split //,$row[0];
                        }
                $counter = 0;
                for(@path)
                        {
                        $filepath .= $path[$counter];
                        if($counter % $fs) { $filepath .= "/"; }
                        $counter++;
                        }
                $filepath =~ s/\/$//;
                if (($fn =~ /StudyDBKeyFileNamingStrategy/) and ($row[3]))
                        {
                        $filepath = "/opt/emageon/archive/dicom/".$dss."/files/".$filepath."/".$sop;
                        }
                elsif (($fn !~ /StudyDBKeyFileNamingStrategy/) and ($row[3]))
                        {
                        $filepath = "/opt/emageon/archive/dicom/".$dss."/files/".$filepath."/".$row[2]."/".$sop;
                        }
                elsif (($isMultiTiered) and ($isMultiTieredNFS) and ($isConcatenatedDate))
                        {
                        $filepath = $MultiTieredNFS_basedir.$spid."/files/".$row[5]."/".$row[2]."/".$row[2]."_".$row[1].".tar";
                        }
                elsif (($isMultiTiered) and ($isMultiTieredNFS) and (!$isConcatenatedDate))
                        {
                        $filepath = $MultiTieredNFS_basedir.$spid."/files/".$row[6]."/".$row[7]."/".$row[8]."/".$row[2]."/".$row[2]."_".$row[1].".tar";
                        }
                else
                        {
                        undef $filepath;
                        }
                if (($isMultiTiered) and ($isMultiTieredNFS) and (!$row[3]))
                        {
                        $results = `tar tvf $filepath | grep $sop | awk '{print \$6}'`;
                        if ($results)
                                {
                                $filepath = $filepath."-".$results;
                                }
                        }
                }
	return $filepath;
	}