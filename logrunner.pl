#!/usr/bin/perl
use strict;
use warnings;

#open syslog 
use Sys::Syslog; #logging facility
openlog("FILEMOVER", "pid", "LOG_USER");

# globals
# ARGV0 is the directory, ARGV1 is the username, ARGV2 is the jobid
my $dir = $ARGV[0];
my $username = $ARGV[1];
my $jobid = $ARGV[2];

if (!$username) {
    FATAL("No user specified when calling logrunner.pl", "");
} 

if (!$dir) {
    FATAL("No cache directory specified when calling logrunner.pl", $username);
} 

#### MAIN ####
if (!$jobid) {
    processRsync($username, $dir);
} else {
    processUserLog($username, $jobid, $dir);
}

### SUBS ###
sub FATAL {
    my $msg = shift;
    my $uname = shift;
    print STDERR "ERROR: $msg\n";
    syslog("crit", "ERROR for user $uname: $msg");
    exit;
}

sub WARN {
    my $msg = shift;
    my $uname = shift;
    print STDERR "WARNING: $msg\n";
    syslog("warn", "ERROR for user $uname: $msg");
}

sub processUserLog {
    my $user = shift;
    my $job = shift;
    my $dir = shift;
    my $filepath = "$dir/filemover_$job.log";

    if (-e $filepath) {
	open (FH, "<", $filepath) or FATAL("Cannot open logfile at $filepath", $user);
    } else {
	FATAL("User logfile does not exist at $filepath", $user);
    }

    my $perfdata = "";
    while (<FH>) {
	if ($_ =~ "PerfStats") {
	    # the line in question looks like
	    # PerfStats: Bytes; 256629018624: Files; 239: Avg file size; 1024 MB: RDMA throughput; 623.93 MB/s: TCP throughput; 0.00 MB/s
	    # it's set up for a split but at this point we are just sending mail
	    $perfdata .= $_; #in case of multiple user groups
	}
    }

    my $address = "bridges-ft\@psc.edu";
    my $subject = "Filemover performance stats for $user";
    my $from = "Filemover\@noreply.psc.edu";
    
    open(my $MAIL, "|/usr/sbin/sendmail -t");
    if (!$MAIL) {
	print "COULD NOT OPEN MAIL!\n";
	syslog("crit". "ERROR: Could not open sendmail to send error message");
	exit;
    }
    
    print $MAIL "To: $address\n";
    print $MAIL "From: $from\n";
    print $MAIL "Subject: $subject\n";
    print $MAIL "The filemover process for $user is complete. Summary performance stats follow.\n";
    print $MAIL "Please review the full log file at $filepath for more details\n\n";
    print $MAIL $perfdata . "\n";
    close ($MAIL);
}

sub processRsync {
    my $user = shift;
    my $dir = shift;

    # define our arrays
    my @missing;
    my @denied;
    my @vanished;
    my @allerrors;
    
    #delete the fpart chache directory 
    while ($_ = glob("$dir/fpcache/*")) {
	    print "$_\n";
	    next if -d $_;
	    unlink($_);
    }
    rmdir "$dir/fpcache";
    
    #remove the suspend log
    unlink "$dir/suspend.log";
    
    #get all of the rsync log files
    my @logfiles = glob("$dir/rsync-logfile*");
    if (scalar @logfiles == 0) {
	FATAL("No rsync log files found at $dir", $user);
    }
    
    #open each file in turn and look for errors
    foreach my $filepath (@logfiles) {    
	my $errorflag = 0;
	#print "Opening $filepath\n";
	open (FH, "<", $filepath) or WARN("Cannot open logfile at $filepath", $user);
	while (<FH>) {
	    #check for file missing before rsync starts
	    if ($_ =~ /No such file/i) {
		chomp;
		my @line = split (':', $_);
		$line[3] =~ /"(.+)"/; #get the file name from between the quotes
		push (@missing, $1);
		$errorflag = 1;
	    }
	    # file goes missing after rsync starts
	    if ($_ =~ /file has vanished/i) {
		chomp;
		my @line = split (":", $_);
		$line[3] =~ /"(.+)"/; #get the file name from between the quotes
		push (@vanished, $1);
		$errorflag = 1;
	    }
	    # cannot create directory on target
	    if ($_ =~ /Permission denied/i) {
		chomp;
		my @line = split (":", $_);
		$line[3] =~ /"(.+)"/; #get the file name from between the quotes
		push (@denied, $1);
		$errorflag = 1;
	    }
	    if ($_ =~ /error/i) {
		chomp;
		my $line = $filepath . " : " . $_;
		push (@allerrors, $line);
		$errorflag = 1;
	    }
	}
	close (FH);
	# no errors found in file so delete it
	if ($errorflag == 0) {
	    unlink($filepath);
	}
    }
    
    open (OF, ">", "$dir/rsync_failure.log") or FATAL("Could not open rsync failure log in $dir", $user);
    
    print OF "rsync failure log for $user\n\n";
    print OF "Vanished:\n";
    if ($#vanished == 0) {
	print OF "No vanished files for $user";
    } else {
	print OF join ("\n", @vanished);
    }
    
    print OF "\nMissing:\n";
    if ($#missing == 0) {
	print OF "No missing files for $user";
    } else {
	print OF join ("\n", @missing);
    }
    
    print OF "\nDenied:\n";
    if ($#missing == 0) {
	print OF "No missing files for $user";
    } else {
	print OF join ("\n", @denied);
    }
    
    print OF "\nAll errors:\n";
    if ($#allerrors == 0) {
	print OF "No errors for $user";
    } else {
	print OF join ("\n", @allerrors);
    }
    close (OF);
}
