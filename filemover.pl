#!/usr/bin/perl
#
#
# Wrapper to transfer user files from one filesystem to another
# using parsyncfp and slurm. Need to take user input - in this case
# a list of directories, confirm that those directories exist and
# are readable, create the required parsyncfp command, generate a slurm
# batch file and then submit the job.
# REQUIRES: Slurm
# OPTIONAL: parsyncfp and fpart
#           other transport methods can be used but will need
#           some handrolled jiggering at this point. Later
#           I might develop methods to allow a user to select one
#           method over another.

# DETERMINE: Do we need to have any sort of reporting or can we count on
#            slurm for that? Mostly to track who has and who hasn't
#            submitted a job. This would be of somewhat limited value
#            as all we will get is when the application was run and
#            who ran it. Meh - just add it to the logs I guess.

use strict;
use warnings;
use Getopt::Std; # user command line parsing
use Config::Tiny; # just in case we need it
use Sys::Syslog qw(:standard);
use Capture::Tiny qw(:all); #get the results of system commands
use File::HomeDir; # get the users home directory. More reliable than env data
use Data::Dumper; # debugging purposes
use Sys::Syslog; #logging facility

openlog("FILEMOVER", "pid", "LOG_USER");

#########################
#                       #
#      GLOBALS          #
#                       #
#########################

my %options; 
my $configure_path = "/pylon5/pscstaff/rapier/filemover/filemover.cfg";
my $config= Config::Tiny->new;
local $| = 1;

########################
#                      #
#      SUBROUTINES     #
#                      #
######################## 
sub print_notice {
    print <<EOF;
Welcome to the PSC file mover. This application will move your files
to the Bridges 2 file storage system as a scheduled slurm job. We can
provide no estimate on when the job will run or how long it will take
move your files. 
EOF
}

# read the configuration file and put everything into
# the global config data structure
sub read_config {
    my $cfg_path = shift @_;
    if (! -e $cfg_path) {
	my $usermsg = "Config file not found at $cfg_path. Exiting.\n";
	my $sysmsg = "Config file not found at $cfg_path.";
	my $user = get_username();
	errorLog ($sysmsg, $usermsg, "crit", $user);	
    } else {
	$config = Config::Tiny->read($cfg_path);
	my $error = $config->errstr();
	if ($error ne "") {
	    my $usermsg = "Error: $error. Exiting.\n";
	    my $sysmsg = "Error: $error. Exiting.";
	    my $user = get_username();
	    errorLog ($sysmsg, $usermsg, "crit", $user);	
	}
    }
    #print Dumper $config;
}

# check the paths provided by the user to see if they exist
# return a list of all paths that are valid
sub check_filepaths {
    my @filelist;
    my $pathbase;
    my @pathargs;
    my %sources;

    #sources hash structure
    # sources -> groupname (char)
    #              ->{'path'} base directory path (char)
    #              ->{'dirs'} array of dirs (array)

    # first we get the base file path
    my $username = get_username();

    # what I didn't know is that people can be members of multiple groups and may have
    # files in multiple places that they need to move. So we need to treat the results 
    # from this like they might be a list of groups separated by newlines
    (my $gid, my $err, my $exit) = capture {
	system("getent group | grep -w $username | cut -d ':' -f1");
    };
    if ($err) {
	my $usermsg = "Error: Unfortunately we cannot determine the what groups you belong to. Please contact $config->{support}->{email}.\n";
	my $sysmsg = "Error: Cannot determine user groups: $err";
	errorLog ($sysmsg, $usermsg, "crit", $username);
    }
    # take the list and split it into an array
    my @groups = split ("\n", $gid);

    #check each entry in the array and see if it corresponds to a valid path
    foreach $gid (@groups) {
	$pathbase = "$config->{filesystem}->{outbound}/$gid/$username";
	if (-e $pathbase) {
	    # it does so add it to our hash
	    $sources{$gid}{'path'} = $pathbase;
	}
    }

    #Whoops they don't have any valid paths that we can find 
    if (scalar keys %sources == 0) {
	my $usermsg = "Error: Unfortunately we cannot find any valid directories correspond to your groups. Please contact $config->{support}->{email}.\n";
	my $sysmsg = "Error: User does not have any directories associated with known user groups: " . join (":", @groups);
	errorLog ($sysmsg, $usermsg, "crit", $username);
    }

    # go through each hash entry and get the list of directories they
    # want to transfer from each root path
    foreach my $group (sort keys %sources) {
	$pathbase = $sources{$group}{'path'};
	print "Enter a comma delimited list of the directories you would like to transfer\n";
	print "from the following base directory. If you would like to transfer everything\n";
	print "enter a blank line. To skip this path enter 'skip'\n";
	print "Base directory: $pathbase\n";
	my $input = <STDIN>;
	chomp $input;
	
	#give them them option to skip this path
	if ($input =~ /skip/i) {
	    delete $sources{$group};
	    next;
	}

	# TODO: we should probably ensure to remove leading and trailing commas
	# it shouldn't mess anything up if we don't as it will just check the 
	# root diretcory which we know exists but 
	@filelist = split /,/, $input;
	
	# they've entered something so validate that these
	# subdirectories exist
	if (scalar @filelist > 0) {
	    my $dircount = scalar @filelist;
	    my $goodcount = $dircount;
	    foreach my $directory (@filelist) {
		$directory = trim($directory);
		if (!-e "$pathbase/$directory") {
		    print "$pathbase/$directory does not exist\n";
		    $goodcount--;
		} else {
		    push @{$sources{$group}{'dirs'}}, $directory;
		}
	    }
	    print "$goodcount of $dircount directories have been validated\n";
	    print "Continue? (Y/n)\n";
	    my $input = <STDIN>;
	    if ($input =~ /n/i) {
		my $usermsg = "Error: You chose to not continue. Your files will not be transferred.\n";
		my $sysmsg = "Error: User halted filemover in check_filepaths.";
		my $user = get_username();
		errorLog ($sysmsg, $usermsg, "warn", $user);
		exit; #explicit exit because 'warn' doesn't exit and we don't want to send mail to support
	    } 
	}
    }

    # after goign through all of that they must have skipped all of their
    # root directories. Whatever. Warn them and exit. 
    if (scalar keys %sources == 0) {
	my $usermsg = "Error: You have not selected any directories to transfer. Your files will not be transferred.\n";
	my $sysmsg = "Error: User didn't select any directories to transfer.";
	my $user = get_username();
	errorLog ($sysmsg, $usermsg, "warn", $user);	
	exit; #explicit exit because 'warn' doesn't exit and we don't want to send mail to support
    }
    return \%sources;
}

sub transport_command {
    my $sources_ref = shift @_; #referenced sources hash from check_filepaths
    my $tool = shift @_;
    my $base;
    my $dirlist;
    my $command;

    if ($tool eq "home") {
	$command = build_pfp("home");
	return $command;
    }

    my %sources = %{$sources_ref};
    if ($tool eq "pfp") {
	foreach my $group (keys %sources) {
	    $base = $sources{$group}{'path'};
	    $dirlist = ""; #have to set it to null otherwise it inherits the last set value
	    if ($sources{$group}{'dirs'}) {
		$dirlist = join " ", @{$sources{$group}{'dirs'}};
	    }
	    # we can now build the parsyncfp directory line
	    # with $base $dirlist and if the dirlist is empty we
	    # still have a a valid argument
	    $command .= build_pfp($base, $dirlist, $group) . "\n\n";
	}
	my $username = get_username();
	$command .= "#copy parsyncfp log file for performance evaluation\n";
	$command .= "cp filemover_*.log $config->{paths}->{cache}/$username.psync.cache/\n\n";
	return $command;
    }
    #if ($tool eq "tarpipe") {
	#create file list
	#while (@paths) {
	 #   if ($#paths > 0) {
	#	$dirlist .= pop @paths;
	#	$dirlist .= " "; #space seperated
	 #   } else {
	#	$base = pop @paths;
	 #   }
	#}
	#$command = build_tarpipe($base, $dirlist);
	#return $command;
    #}
    #if ($tool eq "fpsync") {
#	$command = build_fpsync(\@paths);
#	return $command;
 #   }
}

sub build_pfp {
    my $startdir = shift @_;
    my $dirlist = shift @_;
    my $group = shift @_;
    my $target;
    my $nowait;

    if ($startdir eq "home") {
	$target = $config->{filesystem}->{home};
    } else {
	$target = $config->{filesystem}->{inbound}; # this is just the prefix
    }

    # they haven't specified any directories but we need to provide one
    # so use '.' to move all files in the root
    if (!$dirlist) {
	$dirlist = ".";
    }
    
    if ($config->{parsyncopts}->{nowait} eq "true") {
	$nowait = "--nowait";
    }

    my $username = get_username();

    if ($startdir eq "home") {
	$target = $username .  "\@" . $target . ":/home/" . $username;
	$startdir = "/home";
    } else {
	## this is just for testing at this point ##
	# TODO: we still need to determine how the target is going to
	# be determined. Leave it as is for now
	$target .= "/" . $group . "/" . $username;
    }
    
    my $pfp = <<EOF;
#ensure that the target directory exists
mkdir -p $target

#ensure that the cache diectory exists
mkdir -p $config->{paths}->{cache}/$username.psync.cache/$group

$config->{paths}->{parsyncfp} -NP=$config->{parsyncopts}->{np} \\
--user=$username \\
--spinneroff \\
-maxload=$config->{parsyncopts}->{maxload} \\
-chunksize=$config->{parsyncopts}->{chunk_size} $nowait \\
--rsyncopts='$config->{parsyncopts}->{rsyncopts}' \\
--interface=$config->{parsyncopts}->{interface} \\
--altcache=$config->{paths}->{cache}/$username.psync.cache/$group \\
--startdir='$startdir' $dirlist $target 

#parse the rsync logs for failures. logrunner also clears the rsync logs
$config->{paths}->{parsync_bindir}/logrunner.pl $config->{paths}->{cache}/$username.psync.cache/$group $username
EOF

    $pfp = trim($pfp);
    #print "pfp = $pfp\n";
    return $pfp;
}

# in this case we are building a pretty naive tarpipe just to see what it looks like
# ideally we'd do this with fpart but that can come later
sub build_tarpipe {
    my $base = shift @_;
    my $dirpaths = shift @_;
    my $target = $config->{filesystem}->{inbound}; # this is just the prefix

    ## this is just for testing at this point ##
    # TODO: we still need to determine how the target is going to
    # be determined. Leave it as is for now
    my $username = get_username();
    $target .= "/" . $username;

    my $command = "pwd $base;\\
tar $config->{tarpipeopts}->{tarmakeopts} -cf - $dirpaths | //
tar $config->{tarpipeopts}->{tarextractopts} -xf - -C $target";

    return $command;
}

sub build_fpsync {
    my $paths_ref = shift @_;
    my @paths = @$paths_ref;
    my $target = $config->{filesystem}->{inbound}; # this is just the prefix
    my %src_dirs;
    my $command;

    print "Number of paths is " . $#paths . "\n";

    # we need to build a separate fpsync command for each path
    if ($#paths > 1) {
	my $base = $paths[0]; #get the base directory 
	#print "Base is $base";
	while (@paths) {
	    if ($#paths > 0) {
		my $path = pop @paths;
		$src_dirs{$path} = $base . "/" . $path;
		print "Added " . $src_dirs{$path} . "\n";
	    } else {
		pop @paths; #get rid of the last element in the array (0th index)
	    }
	}
    } else {
	$src_dirs{"."} = $paths[0];
    }

    ## this is just for testing at this point ##
    # TODO: we still need to determine how the target is going to
    # be determined. Leave it as is for now
    my $username = get_username();
    $target .= "/" . $username;

    foreach my $src_dir (keys %src_dirs) {
	#print "new command being added is fpsync -m cpio -n6 -vv -s5368709120 $src_dirs{$src_dir} $target/$src_dir;\n";
	$command .= "fpsync -m cpio -n6 -vv -s5368709120 $src_dirs{$src_dir} $target/$src_dir;\n";
    }
    return $command;
}

# take the incoming filemove command and construct a
# slurm batch job around. Save this as a file in the
# users home directory and return the file path
sub build_slurm_batch {
    my $move_command = shift @_;
    my $home = File::HomeDir->my_home;
    my $batch_path = $home . "/filemover.slurm.sh";

    #get the user email in order to send them notifications
    print "Please enter your email address to receive job notifications.\n";
    print "email address: ";
    my $email = <STDIN>;
    chomp $email;
    $email = trim($email);
    if (!$email) {
	$email = $config->{support}->{email};
    }

    my $username = get_username();

    #build the batch file
    my $batch =  <<EOF;
#!/bin/bash
#SBATCH --job-name=$config->{slurmopts}->{jobname}  
#SBATCH --mail-type=$config->{slurmopts}->{mail_event}
#SBATCH --mail-user=$email
#SBATCH --partition=$config->{slurmopts}->{partition}
#SBATCH --ntasks-per-node=$config->{slurmopts}->{ntasks}
#SBATCH --nodes=$config->{slurmopts}->{nodes}
#SBATCH --time=$config->{slurmopts}->{time}
#SBATCH --output=$config->{slurmopts}->{output}

echo "User              = $username"
echo "Date              = \$(date)"
echo "Hostname          = \$(hostname -s)"
echo "Working Directory = \$(pwd)"
echo ""
echo "Number of Nodes Allocated      = \$SLURM_JOB_NUM_NODES"
echo "Number of Tasks Allocated      = \$SLURM_NTASKS"
echo "Number of Cores/Task Allocated = \$SLURM_CPUS_PER_TASK"

export PATH="\$PATH:$config->{paths}->{parsync_bindir}"
export ANSI_COLORS_DISABLED="true"

$move_command

EOF

    # open the file and write the batch file
    #print $batch;
    open (my $sjh, ">", $batch_path);
    if (!$sjh) {
	my $usermsg = "Error: Cannot write to slurm batch file to $batch_path.\n Please contact support st $config->{support}->{email}\n";
	my $sysmsg = "Error: Cannot write to slurm batch file to $batch_path.";
	my $user = get_username();
	errorLog ($sysmsg, $usermsg, "crit", $user);	
    }
    print $sjh $batch;
    close ($sjh);
    print "Batch file created in $batch_path\n";

    return ($batch_path);
}

#take the path created in the build_slurm_batch function and executes it via sbatch
sub fire_slurm {
    my $path = shift @_;

    print "Submitting batch job found at $path\n";
    (my $result, my $err, my $exit) = capture {
	system("$config->{paths}->{sbatch} $config->{slurmopts}->{reservation} $path")
    };
    if ($result =~ "Submitted batch") {
	$result =~ /(\d+)/;
	return $1;
    } else {
	my $usermsg = "Error: sbatch returned an error: $err.\n Your job has not been submitted.\n Please contact support st $config->{support}->{email}\n";
	my $sysmsg = "Error: sbatch returned an error of $err.";
	my $user = get_username();
	errorLog ($sysmsg, $usermsg, "crit", $user);	
    }
}

sub get_username {
    my $username = capture {system("whoami")};
    if (!$username) {
	my $usermsg = "We can't figure out your username. Please contact $config->{support}->{email}\n";	
	errorLog ("Could not determine username.", $usermsg, "crit", "UNKNOWN");
    }
    return trim($username);
}

sub trim {
        my ($string) = @_;
        $string =~ s/^\s+|\s+$//g;
        return $string;
}

sub usage {
    print "filemover usage\n";
    print "\tfilemover.pl [-d 'comma,sereparated,directory,list'] [-t] [-f] [-h]\n";
    print "\t-d quoted comma separated list of directories to move (relative to \$SCRATCH[?])\n";
    print "\t   these directories must be readable by the user\n";
    print "\t=m Move user home directory\n";
    print "\t-t Use tar and pipe instead of parsyncfp. Not recommended.\n";
    print "\t-f Use fpsync instead of parsyncfp.\n";
    print "\t-h this help text\n";
    exit;
}

# more flexible approach to dealing with errors
# than just writing to syslog
# errorlog(char $sysmsg, char $usermsg, char $level, char $username)
# sysmsg is the message for syslog
# usermsg is the error sent to the console for the user
# level is the syslog error level
# user is the user that started the process
sub errorLog {
    my $sysmsg = shift;
    my $usermsg = shift;
    my $level = shift;
    my $user = shift; #we can't user get_username as if there is an error there then we loop
    print $usermsg . "\n";
    syslog ($level, $sysmsg);
    if ($level eq "crit") {
	mailError($sysmsg, $level, $user);
	print "Filemover is exiting.\n\n";
	exit;
    }
}

# send mail to support to indicate fatal error
sub mailError {
    my $msg = shift;
    my $level = shift;
    my $user = shift;
    my $address = "bridges-ft\@psc.edu";
    my $subject = "$level from Filemover process";
    my $from = "Filemover\@noreply.psc.edu";

    open(my $MAIL, "|/usr/sbin/sendmail -t");
    if (!$MAIL) {
        syslog("crit". "ERROR: Could not open sendmail to send error message");
        exit;
    }
    
    print $MAIL "To: $address\n";
    print $MAIL "From: $from\n";
    print $MAIL "Subject: $subject\n";
    print $MAIL "$user has encountered a problem with the filemover application\n";
    print $MAIL $msg . "\n";
    close ($MAIL);
}

###############################
#                             #
#         MAIN                #
#                             #
###############################

if (!getopts ("btfhm", \%options)) {
    print "You entered an invalid option.\n";
    &usage;
}
if (defined $options{h}) {
    &usage;
}

read_config($configure_path);

# basics have been loaded let syslog know
syslog ("notice", "Started filemover process for: " . get_username());

my $tool = "pfp";
if ($options{t} && $options{f}) {
    print "You cannot select both fpsync and a tar pipe at the same time.\n";
    exit;
}
if ($options{t}) {
    $tool = "tarpipe";
}
if ($options{f}) {
    $tool = "fpsync";
}

if ($options{m}) {
    $tool = "home";
}

print_notice();

my $sources_ref;
if (!$options{m}) {
    $sources_ref = check_filepaths();
}

# if we get here we have *something* in paths_ref even if it's just the base
# directory. Now that we have that we can build the transport command
my $filemove_command = transport_command($sources_ref, $tool);
my $slurm_batch = build_slurm_batch($filemove_command);
# we have the slurm batch file written. Execute it. 
if (!$options{b}) {
    my $slurm_id = fire_slurm($slurm_batch);
    print "Your file transfer job has been submitted to slurm. The slurm job id is $slurm_id\n";
    print "You can track progress in the filemover_" . $slurm_id . ".log file\n";
    syslog ("notice", "Batch job $slurm_id submitted to slurm for user: " . get_username());
} else {
    print "The slurm job batch file has been written to your home directory but not executed\n";
    print "You will need to submit that job manually\n";
    syslog ("notice", "Batch job created but not submitted for user: " . get_username());
}
exit;
