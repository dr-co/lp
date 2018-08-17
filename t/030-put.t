#!/usr/bin/env tarantool

local yaml = require 'yaml'
local msgpack = require 'msgpack'
local test = require('tap').test()
test:plan(11)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

package.path = string.format('%s;%s',
    'lua/?.lua;lua/?/init.lua',
    package.path
)

local lp = require 'lp'
test:ok(lp:init(), 'LP init done')

local task = lp:push('key', 'value')
test:ok(task, 'task was put')

local task2 = lp:push('key', 'value')
test:ok(task2, 'task was put again')

test:is(task2[1], task[1] + 1, 'autoincrement id')

local task3 = lp:push('key', msgpack.null)
test:ok(task3, 'task with no data was put')
test:is(task3[1], task2[1] + 1, 'autoincrement id')
test:ok(task3[4] == nil, 'data is nil')

local cnt = lp:push_list('key1', 'data1', 'key2', 'data2')
test:is(cnt, 2, 'tuples inserted')

task = lp('push', 'a', 'b')
test:ok(task, 'call-style push')

count = lp('push_list', 'a', 'b', 'c', 'd', 'e')
test:is(count, 3, 'push_list')

tnt.finish()
os.exit(test:check() == true and 0 or -1)

