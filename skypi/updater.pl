#!/usr/bin/env perl

use warnings;
use strict;

use POSIX qw(strftime);
use Sys::Hostname;

our $workdir = "$ENV{HOME}/.update";
our $hostname = hostname;

sub fetch_and_maybe_eval {
    my ($filename) = @_;

    `curl -s http://worrall.cc/skypi/$filename -o $filename`;
    (-f $filename) || die "curl failed, looks like\n";

    my @s1 = split(' ', `sum $filename`);
    my @s2 = split(' ', `sum $filename.last`);
    if ($s1[0] eq $s2[0]) {
        unlink ($filename);
        exit 0;
    }

    my $outfile = $filename.".".strftime("%Y%m%d-%H%M%S",gmtime(time()));
    chmod(0755, $filename);
    system("./$filename 1>$outfile 2>&1");

    # Roll scripts
    if (-f "$filename.last") {
        my $suffix = strftime("%Y%m%d-%H%M%S", gmtime((stat "$filename.last")[9]));
        rename("$filename.last", "$filename.$suffix")
    }
    rename ($filename, "$filename.last");

    my $destfile = "skypi\@wormers.net:~/$hostname";
    `scp $outfile $destfile`;
}

eval {
    (-d $workdir) || mkdir($workdir) || die "mkdir $workdir: $!\n";
    chdir($workdir) || die "chdir $workdir: $!\n";

    for my $filename ("updater.$hostname", "updater.all") {
        fetch_and_maybe_eval ($filename);
    }

}; if ($@) {
    print "Aborted with: $@\n";
    exit 1
}
