#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 9;
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
}

my $tserver = DR::Tarantool::StartTest->run(
    cfg         => 'lp.tarantool.cfg',
    script_dir  => rel2abs('t/on_lsn'),
);


my $tnt = DR::Tarantool::CoroClient->connect(
    host    => '127.0.0.1',
    port    => $tserver->primary_port,
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
is_deeply $tnt->call_lua('test_str' => [])->raw, ['str'],
    'test function is present';

is_deeply $tnt->call_lua('test_lsn' => [])->raw, [0],
    'test_lsn init';

$tnt->insert('lp' => [1,2,3,4]);

Coro::AnyEvent::sleep 0.15;

is_deeply $tnt->call_lua('test_lsn' => [])->raw, [2],
    'test_lsn was touched';

