#!/usr/bin/perl

use warnings;
use strict;

use utf8;
use open qw(:std :utf8);
use IO::Socket::INET;
use File::Temp 'tempdir';
use File::Path 'rmtree';

sub free_port {
    for (1 .. 1000) {
        my $s = IO::Socket::INET->new(
            Listen      => 1,
            ReusePort   => 1,
        );
        next unless $s;
        my $port = $s->sockport;
        close $s;
        return $port;
    }
    return undef;
}

my $master = free_port;
my $replica;

do {
    $replica = free_port;
} while $replica == $master;

$ENV{TEST_DIR} = tempdir;
$ENV{MASTER_PORT} = $master;
$ENV{REPLICA_PORT} = $replica;

my $master_lua = 't/100-master-slave.master.lua'; 
my $replica_lua = 't/100-master-slave.replica.lua';

my ($master_pid, $replica_pid);

my $exit_code = 0;

unless ($replica_pid = fork) {
    select undef, undef, undef, 0.3;
    exec tarantool => $replica_lua;
}

if ($master_pid = open my $out, '-|', tarantool => $master_lua) {

    while (<$out>) {
        print;
    }

    waitpid $master_pid, 0;

    $master_pid = undef;
    exit $? >> 8;
}



END {
    for ($replica_pid, $master_pid) {
        next unless $_;
        kill TERM => $_;
        waitpid $_, 0;
        $_ = undef;
    }

    for (qw(master replica)) {
        last;
        my $log = `cat $ENV{TEST_DIR}/$_/tarantool.log`;
        print "=============== $_ log file ====================\n";
        print $log;
    }
    rmtree $ENV{TEST_DIR};
}

exit $exit_code;
