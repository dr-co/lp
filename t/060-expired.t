#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(5)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

package.path = string.format('%s;%s',
    'lua/?.lua;lua/?/init.lua',
    package.path
)

local lp = require 'lp'
test:ok(lp:init({ expire_timeout = 0.2 }), 'LP init done')

test:ok(lp:push('key', 'value'), 'push task')


test:test('take immediatelly', function(test) 
    test:plan(3)
    local list = lp:subscribe(1, 1, 'key')
    test:is(#list, 2, 'one event was fetch immediattely')
    test:is(list[1][4], 'value', 'event data')

    test:is(list[#list][1], list[1][1] + 1, 'next id')
end)

fiber.sleep(0.21)

test:test('expire process removed task', function(test) 
    test:plan(3)
    local list = lp:subscribe(1, 1, 'key')
    test:is(#list, 1, 'no one event was fetch')
    test:is(list[1][1], list[1][2], 'database empty')
    test:ok(list[1][1] > 1, 'id more than one')
end)

-- test:diag(tnt.log())
tnt.finish()
os.exit(test:check() == true and 0 or -1)

