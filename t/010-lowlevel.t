#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 54;
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
    script_dir  => rel2abs('.'),
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

        100 => {
                default_type => 'UTF8STR',
                name => 'old_lp',
                fields => [
                    {
                        name => 'id',
                        type => 'NUM64',
                    },
                    {
                        name => 'e1',
                        type => 'UTF8STR',
                    },
                    {
                        name => 'e2',
                        type => 'UTF8STR',
                    },
                    {
                        name => 'e3',
                        type => 'UTF8STR',
                    },
                    {
                        name => 'e4',
                        type => 'UTF8STR',
                    },
                    {
                        name => 'e5',
                        type => 'UTF8STR',
                    },
                    {
                        name => 'type',
                        type => 'STR',
                    },
                    {
                        name => 'klen',
                        type => 'NUM',
                    },
                    {
                        name => 'created',
                        type => 'NUM64',
                    },
                    {
                        name => 'data',
                        type => 'UTF8STR',
                    },
                ],
                indexes => {
                    0 => 'id',
                }
        }
    },
);

ok $tnt->ping, 'tnt->ping';

{
    my $task1 =  $tnt->call_lua('lp.put', [ 'test', '"data1"' ] => 'lp');
    is $task1->data, 'data1', 'task->data';


    my $task2 =  $tnt->call_lua('lp.put', [ 'test', '"data2"' ] => 'lp');
    is $task2->data, 'data2', 'task->data';

    is $task1->id, $task2->id - 1, 'task->id';
    cmp_ok time(), '>=', $task1->time, 'task->time';
    cmp_ok time(), '>=', $task2->time, 'task->time';

    note 'id == 0';
    my $list1 = $tnt->call_lua('lp.take', [ 1, .1, 'abc', 'test' ] => 'lp');
    is $list1->iter->count, 3, '3 items';
    is $list1->iter->get(-1)->id, $task2->id + 1, 'last id';
    is $list1->iter->get(-1)->key, undef, 'no key in last tuple';
    is $list1->iter->get(0)->key, 'test', 'key1';
    is $list1->iter->get(1)->key, 'test', 'key2';
    is $list1->iter->get(0)->data, 'data1', 'data1';
    is $list1->iter->get(1)->data, 'data2', 'data2';
   
    note 'id > 0';
    my $list2 = $tnt->call_lua('lp.take',
        [ $task1->id, .1, 'abc', 'test' ] => 'lp');
    is $list2->iter->count, 3, '3 items';
    is $list2->iter->get(-1)->id, $task2->id + 1, 'last id';
    is $list2->iter->get(-1)->key, undef, 'no key in last tuple';
    is $list2->iter->get(0)->key, 'test', 'key1';
    is $list2->iter->get(1)->key, 'test', 'key2';
    is $list2->iter->get(0)->data, 'data1', 'data1';
    is $list2->iter->get(1)->data, 'data2', 'data2';

    note 'id > max';
    my $list3 = $tnt->call_lua('lp.take', [ $task2->id + 1, .1, 'test' ], 'lp');
    is $list3->iter->count, 1, 'items';
    is $list3->iter->get(-1)->id, $task2->id + 1, 'last id';
    is $list3->iter->get(-1)->key, undef, 'no key in last tuple';
    
    $list3 = $tnt->call_lua('lp.take', [ 0, .1, 'test' ], 'lp');
    is $list3->iter->count, 1, 'items';
    is $list3->iter->get(-1)->id, $task2->id + 1, 'last id';
    is $list3->iter->get(-1)->key, undef, 'no key in last tuple';

    note 'id > min';
    my $list4 = $tnt->call_lua('lp.take', [ $task1->id + 1, .1, 'test' ], 'lp');
    is $list4->iter->count, 2, 'items';
    is $list4->iter->get(-1)->id, $task2->id + 1, 'last id';
    is $list4->iter->get(-1)->key, undef, 'no key in last tuple';
    is $list4->id, $task2->id, 'id';

    note 'expiration tests';
    Coro::AnyEvent::sleep 2.2;
    my $list_e = $tnt->call_lua('lp.take', [ 1, .1, 'abc', 'test' ] => 'lp');
    is $list_e->iter->count, 1, 'all items were deleted by expire fiber';
}

{
    is $tnt->call_lua('lp.expire_timeout', [])->raw(0), '2', 'expire_timeout';
    is $tnt->call_lua('lp.expire_timeout', [50])->raw(0), 50, 'expire_timeout';
    is $tnt->call_lua('lp.expire_timeout', [])->raw(0),  50, 'expire_timeout';

    my $list = $tnt->call_lua('lp.take', [ 1, .1, 'test' ], 'lp');
    is $list->iter->count, 1, 'no tasks in space';
    is $list->id, 3, 'last id';

    async {
        my $list = $tnt->call_lua('lp.take', [ 3, 3, 'test1', 'test3' ], 'lp');
        note 'take (woke up)';
        is $list->iter->count, 3, 'items';
    };

    Coro::AnyEvent::sleep .4;
    note 'put tasks';
    is_deeply
        $tnt->call_lua('lp.put_list',
            [ 'test1', 1, 'test2', 2, 'test3', 3 ], 'lp')->raw,
        [ 3 ],
        'put_list';
}

{
    my $tp = $tnt->call_lua('lp.old_put',
        [ 2, 'abc', 'cde', 'data' ], 'old_lp');


    my $tpc = $tnt->call_lua(
        'lp.old_take', [ $tp->id, 10, 2 => 'abc', 'cde' ], 'old_lp');
    is_deeply $tpc->raw, $tp->raw, 'put and take tuples are the same';

    my $tpcto = $tnt->call_lua(
        'lp.old_take', [ $tp->id + 2, .1, 2 => 'abc', 'cde' ], 'old_lp');
    
    is $tpcto->data, undef, 'data (timeout)';
    is $tpcto->type, 't', 'type (timeout)';
    is $tpcto->klen, 0, 'key length (timeout)';
    is $tpcto->iter->count, 1, 'count of events (timeout)';
}
{
    my $tp = $tnt->call_lua('lp2.put',
        [ 2, 'ab', 'cde', 'data' ], 'old_lp');


    my $tpc = $tnt->call_lua(
        'lp2.take', [ $tp->id, 10, 2 => 'ab', 'cde' ], 'old_lp');
    is_deeply $tpc->raw, $tp->raw, 'put and take tuples are the same';

    my $tpcto = $tnt->call_lua(
        'lp2.take', [ $tp->id + 2, .1, 2 => 'ab', 'cde' ], 'old_lp');
    
    is $tpcto->data, undef, 'data (timeout)';
    is $tpcto->type, 't', 'type (timeout)';
    is $tpcto->klen, 0, 'key length (timeout)';
    is $tpcto->iter->count, 1, 'count of events (timeout)';
}

{
    my %stat = map { ($_->raw(0), $_->raw(1)) }
        $tnt->call_lua('lp.stat', [])->iter->all;
    ok exists $stat{clients}, 'clients in stat';
}

END{
#     note $tserver->log;
}
