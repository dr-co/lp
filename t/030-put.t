#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
test:plan(9)

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


local task3 = lp:push('key')
test:ok(task3, 'task with no data was put')
test:is(task3[1], task2[1] + 1, 'autoincrement id')
test:isnil(task3[4], 'data is nil')

local cnt = lp:push_list('key1', 'data1', 'key2', 'data2')
test:is(cnt, 2, 'tuples inserted')

tnt.finish()
os.exit(test:check() == true and 0 or -1)

