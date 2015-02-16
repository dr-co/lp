#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 1017;
use Encode qw(decode encode);


BEGIN {
    # Подготовка объекта тестирования для работы с utf8
    my $builder = Test::More->builder;
    binmode $builder->output,         ":utf8";
    binmode $builder->failure_output, ":utf8";
    binmode $builder->todo_output,    ":utf8";

    use_ok 'DR::Tarantool::StartTest';
    use_ok 'DR::Tarantool::CoroClient';
    use_ok 'Coro';
    use_ok 'Coro::AnyEvent';
    use_ok 'AnyEvent';
    use_ok 'File::Spec::Functions', 'rel2abs';
    use_ok 'Coro::Channel';
}

my $replica_port = DR::Tarantool::StartTest->_find_free_port;

my $master = DR::Tarantool::StartTest->run(
    cfg                 => 'lp.tarantool.cfg',
    script_dir          => rel2abs('.'),
    replication_port    => $replica_port,
);

ok $master => 'Master is started';
my $replica = DR::Tarantool::StartTest->run(
    cfg                 => 'lp.tarantool.cfg',
    script_dir          => rel2abs('.'),
    replication_source  => sprintf('%s:%s', '127.0.0.1', $replica_port),
);

ok $replica => 'Replica is started';
Coro::AnyEvent::sleep .5;
like $replica->log, qr{successfully connected to master}, 'connected';

my $tnt = DR::Tarantool::CoroClient->connect(
    host    => '127.0.0.1',
    port    => $master->primary_port,
    spaces  => {
        0   => {
            default_field_type => 'STR',
            name    => 'lp',
            fields  => [
                { name  => 'id',        type => 'NUM64'  },
                { name  => 'time',      type => 'NUM'    },
                { name  => 'key',       type => 'STR'    },
                { name  => 'data',      type => 'JSON'   },
            ],

            indexes => {
                0   => 'id',
                1   => [ 'key', 'id' ]
            }
        },
    },
);
ok $tnt->ping, 'tnt->ping';
ok $tnt->call_lua('lp.expire_timeout', [ 3600 ]), 'clean expire_timeout';

my $replica2 = DR::Tarantool::StartTest->run(
    cfg                 => 'lp.tarantool.cfg',
    script_dir          => rel2abs('.'),
    replication_source  => sprintf('%s:%s', '127.0.0.1', $replica_port),
);

ok $replica2 => 'Replica is started';
Coro::AnyEvent::sleep .5;
like $replica2->log, qr{successfully connected to master}, 'connected';

my $tntr = DR::Tarantool::CoroClient->connect(
    host    => '127.0.0.1',
    port    => $replica->primary_port,
    spaces  => {
        0   => {
            default_field_type => 'STR',
            name    => 'lp',
            fields  => [
                { name  => 'id',        type => 'NUM64'  },
                { name  => 'time',      type => 'NUM'    },
                { name  => 'key',       type => 'STR'    },
                { name  => 'data',      type => 'JSON'   },
            ],

            indexes => {
                0   => 'id',
                1   => [ 'key', 'id' ]
            }
        },
    },
);
ok $tntr->ping, 'tntr->ping';

my $tntr2 = DR::Tarantool::CoroClient->connect(
    host    => '127.0.0.1',
    port    => $replica2->primary_port,
    spaces  => {
        0   => {
            default_field_type => 'STR',
            name    => 'lp',
            fields  => [
                { name  => 'id',        type => 'NUM64'  },
                { name  => 'time',      type => 'NUM'    },
                { name  => 'key',       type => 'STR'    },
                { name  => 'data',      type => 'JSON'   },
            ],

            indexes => {
                0   => 'id',
                1   => [ 'key', 'id' ]
            }
        },
    },
);
ok $tntr2->ping, 'tntr2->ping';

$tnt->call_lua('lp.push', [ 'test1', "-1" ] => 'lp');
$tnt->call_lua('lp.push', [ 'test3', "-1" ] => 'lp');


note 'master-replica test';
my $id = 1;
for my $channel (Coro::Channel->new(128)) {

    my $attempts = 500;
    my $done = 0;
    my $eperiod = .05;
    my %sent;

    async {
        for (1 .. $attempts) {
            Coro::AnyEvent::sleep rand $eperiod;
            $sent{$_} = 1;
            ok $tnt->call_lua('lp.push', [ 'test2', "$_" ] => 'lp'), "task $_ put";
        }
        $channel->put(1);
    };

    async {
        for (1 .. 10 +  $attempts) {
            Coro::AnyEvent::sleep rand $eperiod;

            $tnt->call_lua('lp.push', [ 'test1', "$_" ] => 'lp');
            $tnt->call_lua('lp.push', [ 'test3', "$_" ] => 'lp');
        }
        $channel->put(1);
    };

    my $started = AnyEvent::now();
    my $time = 1; 

    my @rl = ($tntr, $tntr2);
    while ($done < $attempts) {
        my $r = shift @rl;
        push @rl => $r;
        my $list = $r->call_lua('lp.subscribe',
            [$time, 10, 'abc', 'test2'] => 'lp');
        Coro::AnyEvent::sleep rand $eperiod;


        for my $task ($list->iter->all) {
            if ($task->data) {
                ok delete($sent{ $task->data }), 'sent task found ' . $task->data;
                $done++;
                next;
            }
            $time = $task->raw(0);
            note 'reply received localhost:' . $r->_llc->{port}
                . ' sent ' . scalar keys %sent;
        }

        last if AnyEvent::now() - $started > $attempts * $eperiod + 5;
        last if $done == $attempts;
    }

    $channel->get;
    $channel->get;
    
    unless (is_deeply \%sent, {}, 'sent list is empty') {
        note $replica->log;
        note $replica2->log;
    }

}


