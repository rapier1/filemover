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

openlog("filemover", "nowait,pid", "user");

#########################
#                       #
#      GLOBALS          #
#                       #
#########################

my %options; 
my $configure_path = "/home/rapier/filemover/filemover.cfg";
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
	print STDERR "Config file not found at $cfg_path. Exiting.\n";
	syslog ("crit", "Config file not found at $cfg_path. Exiting.");
	exit;
    } else {
	$config = Config::Tiny->read($cfg_path);
	my $error = $config->errstr();
	if ($error ne "") {
	    print STDERR "Error: $error. Exiting.\n";
	    syslog("crit", "Error: $error. Exiting.");
	    exit;
	}
    }
    #print Dumper $config;
}

# check the paths provided by the user to see if they exist
# return a list of all paths that are valid
sub check_filepaths {
    my $path = shift @_;
    my @filelist;
    my $pathbase;
    my @pathargs;
    
    # first we get the base file path
    my $username = capture {system("whoami")};
    $username = trim($username);
    if (!$username) {
	print "We can't figure out your username. ";
	print "Please contact $config->{support}->{email}\n";
	exit;
    }
    (my $gid, my $err, my $exit) = capture {
	system('getent group | grep ^`whoami` | cut -d ":" -f3');
    };
    if (!$err) {
	chomp $gid;
	$pathbase = "$config->{filesystem}->{outbound}/$gid/$username";
	print "Your group ID is $gid\n";
	print "We will use $pathbase ";
	print "as the base directory for your files\n";
	if (! -e $pathbase) {
	    print "Unfortunately this directory does not exist. ";
	    print "Please contact $config->{support}->{email}.\n";
	    #exit; #reenable for deployment
	} else {
	    push @pathargs, $pathbase;
	}
    }
    
    # path may not be defined in which case we use the default user directory
    # as the base
    if ($path) {
	# $path may be a CSV of directories
	@filelist = split /,/, $path;
    } else {
	return \@pathargs;
    }
    my $dircount = $#filelist + 1;
    my $goodcount = $dircount;
    foreach my $directory (@filelist) {
	$directory = trim($directory);
	if (!-e "$pathbase/$directory") {
	    print "$pathbase/$directory does not exist\n";
	    $goodcount--;
	} else {
	    push @pathargs, $directory;
	}
    }
    print "$goodcount of $dircount directories have been validated\n";
    print "Continue? (Y/n)\n";
    my $input = <STDIN>;
    if ($input =~ /n/i) {
	print "Process halted.\n";
	exit;
    } else {
	return \@pathargs;
    }
}

sub transport_command {
    my $paths_ref = shift @_; #referenced paths arrays from check_filepaths
    my @paths = @{$paths_ref};
    my $tool = shift @_;
    my $base;
    my $dirlist;
    
    if ($tool eq "pfp") {
	# pop things off the end of the array
	# until we get the 0th element and pop that
	# into the base directory variable
	while (@paths) {
	    if ($#paths > 0) {
		$dirlist .= pop @paths;
		$dirlist .= " "; #space seperated
	    } else {
		$base = pop @paths;
	    }
	}
	
	# we can now build the parsyncfp directory line
	# with $base $dirlist and if the dirlist is empty we
	# still have a a valid argument
	my $command = build_pfp($base, $dirlist);
	return $command;
    }
    # other tools can go here
    
}

sub build_pfp {
    my $startdir = shift @_;
    my $dirlist = shift @_;
    my $target = " ";
    my $nowait;

    if (!$dirlist) {
	$dirlist = " ";
    }
    
    if ($config->{parsyncopts}->{nowait} eq "true") {
	$nowait = "--nowait";
    }

    # we still need to determine how the target is going to
    # be determined. Leave it as is for now
    
    my $pfp = <<EOF;
$config->{paths}->{parsyncfp} -NP=$config->{parsyncopts}->{np} \\
-chunksize=$config->{parsyncopts}->{chunk_size} $nowait \\
--rsyncopts='$config->{parsyncopts}->{rsyncopts}' \\
--startdir='$startdir' $dirlist $target
EOF
    $pfp = trim($pfp);
    print "pfp = $pfp\n";
    return $pfp;
}

# take the incoming filemove command and construct a
# slurm batch job around. Save this as a file in the
# users home directory and return the file path
sub build_slurm_batch {
    my $move_command = shift @_;
    my $home = File::HomeDir->my_home;
    my $batch_path = $home . "/filemover.slurm.sh";

    #get the user email in order to send them notifications
    print "Please enter you email address to receive job notifications.\n";
    print "email address: ";
    my $email = <STDIN>;
    chomp $email;
    $email = trim($email);
    if (!$email) {
	$email = $config->{support}->{email};
    }

    my $username = capture {system("whoami")};
    $username = trim($username);
    if (!$username) {
	print "We can't figure out your username. ";
	print "Please contact $config->{support}->{email}\n";
	exit;
    }

    #build the batch file
    my $batch =  <<EOF;
#!/bin/bash
#SBATCH --job-name=$config->{slurmopts}->{jobname}  
#SBATCH --mail-type=$config->{slurmopts}->{mail_event}
#SBATCH --mail-user=$email
#SBATCH --ntasks=$config->{slurmopts}->{ntasks}
#SBATCH --cpus-per-task=$config->{slurmopts}->{cpus}
#SBATCH --nodes=$config->{slurmopts}->{nodes}
#SBATCH --output=$config->{slurmopts}->{output}

echo "User              = $username
echo "Date              = \$(date)"
echo "Hostname          = \$(hostname -s)"
echo "Working Directory = \$(pwd)"
echo ""
echo "Number of Nodes Allocated      = \$SLURM_JOB_NUM_NODES"
echo "Number of Tasks Allocated      = \$SLURM_NTASKS"
echo "Number of Cores/Task Allocated = \$SLURM_CPUS_PER_TASK"

$move_command

EOF

    # open the file and write the batch file
    print $batch;
    open (my $sjh, ">", $batch_path) or die "Cannot write to $batch_path. Exiting.";
    print $sjh $batch;
    close ($sjh);

    return ($batch_path);
}


sub trim {
        my ($string) = @_;
        $string =~ s/^\s+|\s+$//g;
        return $string;
}

###############################
#                             #
#         MAIN                #
#                             #
###############################

getopts ("f:h", \%options);
if (defined $options{h}) {
    print "filemover usage\n";
    print "\tfilemover.pl [-f] [-h]\n";
    print "\t-f quoted comma separated list of directories to move (relative to \$SCRATCH)\n";
    print "\t\t these directories must be readable by the user\n";
    print "\t-h this help text\n";
    exit;
}

read_config($configure_path);

print_notice();
my $paths_ref = check_filepaths($options{f});
# if we get here we have *something* in paths_ref even if it's just the base
# directory. Now that we have that we can build the transport command
my $filemove_command = transport_command($paths_ref, "pfp");
my $slurm_batch = build_slurm_batch($filemove_command);
