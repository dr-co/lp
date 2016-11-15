#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(6)

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


test:test('take immediatelly', function(test) 
    test:plan(3)
    local list = lp:subscribe(1, 0, 'key')
    test:is(#list, 2, 'one event was fetch immediattely')
    test:is(list[1][4], 'value', 'event data')

    test:is(list[#list][1], list[1][1] + 1, 'next id')
end)


test:test('take timeout', function(test)
    test:plan(2)

    local started = fiber.time()

    local list = lp:subscribe(1, 0.1, 'key1')
    test:is(#list, 1, 'no events are fetched')
    test:ok(fiber.time() - started >= 0.1, 'timeout reached')
end)


test:test('some keys', function(test) 

    box.space.LP:truncate()

    test:plan(3)
    test:is(
        lp:push_list(
            'key1', 'value1',
            'key2', 'value2',
            'key3', 'value3'
        ),
        3,
        '3 tasks were put')

    local list = lp:subscribe(1, 0.1, 'key2')
    test:is(#list, 1, 'no one event was fetched')

    list = lp:subscribe(list[1][2], 0.1, 'key2')
    test:is(#list, 2, 'no one event was fetched')

    

end)

-- test:diag(yaml.encode(box.space.LP:select()))
-- test:diag(tnt.log())
tnt.finish()
os.exit(test:check() == true and 0 or -1)

