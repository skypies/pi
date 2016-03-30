#!/usr/bin/env perl

use warnings;
use strict;

use POSIX qw(strftime);

our $workdir = "$ENV{HOME}/skypi/.update";
our $updater = "updater";

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

    chmod(0755, $updater);
    system("./$updater");

    if (-f "$updater.last") {
        my $suffix = strftime("%Y%m%d-%T", gmtime((stat "$updater.last")[9]));
        rename("$updater.last", "$updater.$suffix")
    }
    rename ($updater, "$updater.last");

}; if ($@) {
    print "Aborted with: $@\n";
    exit 1
}
