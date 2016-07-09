#!/usr/bin/env perl

use warnings;
use strict;

use POSIX qw(strftime);
use Sys::Hostname;

our $workdir = "$ENV{HOME}/.update";
our $hostname = hostname;
our $updater = "updater.$hostname";

eval {
    (-d $workdir) || mkdir($workdir) || die "mkdir $workdir: $!\n";
    chdir($workdir) || die "chdir $workdir: $!\n";

    `curl -s http://worrall.cc/skypi/$updater -o $updater`;
    (-f $updater) || die "curl failed, looks like\n";

    my @s1 = split(' ', `sum $updater`);
    my @s2 = split(' ', `sum $updater.last`);
    if ($s1[0] eq $s2[0]) {
        unlink ($updater);
        exit 0;
    }

    my $outfile = "updater.".strftime("%Y%m%d-%H%M%S",gmtime(time()));
    chmod(0755, $updater);
    system("./$updater 1>$outfile 2>&1");

    # Roll scripts
    if (-f "$updater.last") {
        my $suffix = strftime("%Y%m%d-%H%M%S", gmtime((stat "$updater.last")[9]));
        rename("$updater.last", "$updater.$suffix")
    }
    rename ($updater, "$updater.last");

    my $destfile = "skypi\@wormers.net:~/$hostname";
    `scp $outfile $destfile`;

}; if ($@) {
    print "Aborted with: $@\n";
    exit 1
}
