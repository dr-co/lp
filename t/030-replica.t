#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 43;
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


note 'master-replica test';
my $id = 1;
for my $channel (Coro::Channel->new(128)) {
    my $task =  $tnt->call_lua('lp.push', [ 'test', '"data1"' ] => 'lp');
    is $task->data, 'data1', 'task->data';

    async {
        my $list =
            $tnt->call_lua('lp.subscribe', [ 1, .1, 'abc', 'test' ] => 'lp');
        note 'master';
        is $list->iter->count, 2, '2 items';

        is_deeply [ @{ $list->iter->item(0)->raw }[2, 3] ], ['test', 'data1'],
            'event data';
        $id = $list->iter->item(-1)->id;
        isnt $id, 1, 'id is incremented';
        $channel->put(1)
    };

    async {
        my $list =
            $tntr->call_lua('lp.subscribe', [1, .1, 'abc', 'test'] => 'lp');
        note 'replica';
        is $list->iter->count, 2, '2 items';
        is_deeply [ @{ $list->iter->item(0)->raw }[2, 3] ], ['test', 'data1'],
            'event data';
        is $list->iter->item(-1)->id, $id, 'id is incremented, too';
        $channel->put(1)
    };

    $channel->get for 1 .. 2;

}

note 'revert call order';
for my $channel (Coro::Channel->new(128)) {

    my $count = 0;
    async {
        my $list =
            $tnt->call_lua('lp.subscribe', [ 0, .2, 'abc', 'test' ] => 'lp');
        note 'master received task';
        is $list->iter->count, 2, '2 items';

        is_deeply [ @{ $list->iter->item(0)->raw }[2, 3] ], ['abc', 'cde'],
            'event data';
        isnt $list->iter->item(-1)->id, $id, 'id is incremented';

        $channel->put(1);
    }, $count++;
    async {
        my $list =
            $tnt->call_lua('lp.subscribe', [ $id, .2, 'abc', 'test' ] => 'lp');
        note 'master received task since id=' . $id;
        is $list->iter->count, 2, '2 items';

        is_deeply [ @{ $list->iter->item(0)->raw }[2, 3] ], ['abc', 'cde'],
            'event data';
        isnt $list->iter->item(-1)->id, $id, 'id is incremented';

        $channel->put(1);
    }, $count++;
    async {
        my $list =
            $tnt->call_lua('lp.subscribe',
                [ $id + 1, .2, 'abc', 'test' ] => 'lp');
        note 'master received responce since id=' . ($id + 1);
        is $list->iter->count, 1, '1 (no) items';
        is $list->iter->item(-1)->id, $id + 1, 'id is incremented';
        $channel->put(1);
    }, $count++;

    async {
        my $list =
            $tntr->call_lua('lp.subscribe', [ 0, .2, 'abc', 'test' ] => 'lp');
        note 'replica received task';
        is $list->iter->count, 2, '2 items';

        is_deeply [ @{ $list->iter->item(0)->raw }[2, 3] ], ['abc', 'cde'],
            'event data';
        isnt $list->iter->item(-1)->id, $id, 'id is incremented';
        $channel->put(1);
    }, $count++;
    async {
        my $list =
            $tntr->call_lua('lp.subscribe', [ $id, .2, 'abc', 'test' ] => 'lp');
        note 'replica received task since id=' . $id;
        is $list->iter->count, 2, '2 items';

        is_deeply [ @{ $list->iter->item(0)->raw }[2, 3] ], ['abc', 'cde'],
            'event data';
        isnt $list->iter->item(-1)->id, $id, 'id is incremented';
        $channel->put(1);
    }, $count++;
    async {
        my $list =
            $tnt->call_lua('lp.subscribe',
                [ $id + 1, .2, 'abc', 'test' ] => 'lp');
        note 'replica received responce since id=' . ($id + 1);
        is $list->iter->count, 1, '1 (no) items';
        is $list->iter->item(-1)->id, $id + 1, 'id is incremented';
        $channel->put(1);
    }, $count++;

    Coro::AnyEvent::sleep .1;

    my $task =  $tnt->call_lua('lp.push', [ 'abc', '"cde"' ] => 'lp');
    is $task->data, 'cde', 'task->data';
    
    ok $channel->get, "async $_ is done", for 1 .. $count;
}

note 'max id';
{
    my $list =
        $tnt->call_lua('lp.subscribe',
            [ 517, .2, 'abc', 'test' ] => 'lp');

    is $list->iter->count, 1, '1 (no) items';
    is $list->id, 517, 'id is not changed';
}

