#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(3)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

package.path = string.format('%s;%s',
    'lua/?.lua;lua/?/init.lua',
    package.path
)

local lp = require 'lp'
test:ok(lp:init(), 'LP init done')

fiber.create(function()
    fiber.sleep(0.2)
    lp:push('key', 'value')
end)


test:test('take sleep', function(test) 
    test:plan(5)
    local started = fiber.time()
    local list = lp:subscribe(1, 1, 'key')
    test:ok(fiber.time() - started >= 0.15, 'delay lo')
    test:ok(fiber.time() - started <= 0.25, 'delay hi')
    test:is(#list, 2, 'one event was fetch immediattely')
    test:is(list[1][4], 'value', 'event data')

    test:is(list[#list][1], list[1][1] + 1, 'next id')
end)


-- test:diag(tnt.log())
tnt.finish()
os.exit(test:check() == true and 0 or -1)

