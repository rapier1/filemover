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

#only here for ease of changing. Yes, it should be in the subs. 
my $errfile = "/pylon5/pscstaff/parsync/results/rsyncerrors.log";
my $successfile = "/pylon5/pscstaff/parsync/results/fmsuccess.log";


if (!$username) {
    FATAL("No user specified when calling logrunner.pl", "");
} 

if (!$dir) {
    FATAL("No cache directory specified when calling logrunner.pl", $username);
} 

#### MAIN ####
my $error;
my $rsyncmsg;
my $logmsg;
my $subject;

if (!$jobid) {
    $rsyncmsg = processRsync($username, $dir);
    if ($rsyncmsg) {
	$subject = "ERROR: Rsync errors for $username";
	mailResults($subject, $rsyncmsg);
	writeRsyncErrors($username);
    }
} else {
    $logmsg =  "The filemover process for $username is complete.\n";
    $logmsg .= "$rsyncmsg\n";
    $logmsg .= " Summary performance stats follow.\n";
    $logmsg .= "Please review the full log file at $dir/filemover_$jobid.log for more details\n\n";
    $logmsg .= processUserLog($username, $jobid, $dir);
    $subject = "SUCCESS: Filemover Perfomance Stats for $username";
    mailResults($subject, $logmsg);
    writeFMSuccess($username, $logmsg);
}

exit;

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
    syslog("warning", "ERROR for user $uname: $msg");
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
	    # it's set up for a split but at this point we are just sending mail. Eventually I'd like to put this
	    # all in a database. 
	    $perfdata .= $_; #in case of multiple user groups
	}
    }
    return $perfdata;
}

sub writeRsyncErrors {
    my $user = shift;
    my $msg = "ERROR: Rsync errors in $dir";
    open (FH, ">>", $errfile) or FATAL ($user, "Cannot open $errfile for updates");
    print FH "$user:$msg\n";
    close (FH);
    return;
}

sub writeFMSuccess {
    my $user = shift;
    my $msg = shift;
    chomp $msg; # just in case
    open (FH, ">>", $successfile) or FATAL ($user, "Cannot open $successfile for updates");
    print FH "$user:$msg\n";
    close (FH);
    return;
}

sub mailResults {
    my $subject = shift;
    my $msg = shift;

    my $address = "bridges-ft\@psc.edu";
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
    print $MAIL $msg . "\n";
    close ($MAIL);
    return;
}

sub processRsync {
    my $user = shift;
    my $dir = shift;

    # define our arrays
    my @missing;
    my @denied;
    my @vanished;
    my @allerrors;
    my $errorflag;
    
    #delete the fpart chache directory 
    while ($_ = glob("$dir/fpcache/*")) {
	    next if -d $_;
	    unlink($_);
    }

    #remove the empty fpart cache directories
    rmdir "$dir/fpcache/hold";
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
	$errorflag = 0;
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
    
    $errorflag = 0; #reuse var for email report
    open (OF, ">", "$dir/rsync_failure.log") or FATAL("Could not open rsync failure log in $dir", $user);
    
    print OF "rsync failure log for $user\n\n";
    print OF "Vanished:\n";
    if (scalar @vanished == 0) {
	print OF "No vanished files for $user";
    } else {
	print OF join ("\n", @vanished);
	$errorflag = 1;
    }
    
    print OF "\nMissing:\n";
    if (scalar @missing == 0) {
	print OF "No missing files for $user";
    } else {
	print OF join ("\n", @missing);
	$errorflag = 1;
    }
    
    print OF "\nDenied:\n";
    if (scalar @denied == 0) {
	print OF "No missing files for $user";
    } else {
	print OF join ("\n", @denied);
	$errorflag = 1;
    }
    
    print OF "\nAll errors:\n";
    if (scalar @allerrors == 0) {
	print OF "No errors for $user";
    } else {
	print OF join ("\n", @allerrors);
	$errorflag = 1;
    }
    close (OF);

    if ($errorflag) {
	return ("Errors encountered in rsync log files. Please review rsync_failure.log in\n $dir\n");
    } 

    return;
}
