#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
local fiber = require 'fiber'
test:plan(8)

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
    test:is(#list, 2, 'one event was fetched')
end)

test:test('take after sleep', function(test)
    
    test:plan(12)

    test:diag(lp:_last_id())

    local count = 0
    fiber.create(function()
        count = count + 1
        local list = lp:subscribe(lp:_last_id(), 2, 'a', 'b', 'c')
        test:is(#list, 2, 'event received')
        count = count + 10
        test:is(list[1][3], 'a', 'key')
        test:is(list[1][4], 'da', 'data')
    end)
    
    fiber.create(function()
        count = count + 1
        local list = lp:subscribe(lp:_last_id(), 2, 'd', 'e', 'f')
        test:is(#list, 2, 'event received')
        count = count + 10
        test:is(list[1][3], 'e', 'key')
        test:is(list[1][4], 'de', 'data')
    end)
    
    fiber.create(function()
        count = count + 1
        local list = lp:subscribe(lp:_last_id(), 2, 'g', 'h', 'i')
        test:is(#list, 2, 'event received')
        count = count + 10
        test:is(list[1][3], 'i', 'key')
        test:is(list[1][4], 'di', 'data')
    end)


    local started = fiber.time()
    fiber.sleep(0.2)
    test:is(count, 3, 'all fibers started')
    lp:push('a', 'da')
    lp:push('e', 'de')
    lp:push('i', 'di')

    for i = 1, 10 do
        if count == 33 then
            break
        end

        fiber.sleep(0.09)
    end
    test:ok(fiber.time() - started >= 0.2, 'time lo')
    test:ok(fiber.time() - started <= 0.5, 'time hi')

end)


test:test('id > _last_id', function(test)
    test:plan(7)

    local id = lp:_last_id() + tonumber(10)

    local list = lp:subscribe(id, 0.1, 'a', 'b', 'c')
    test:is(#list, 1, 'no tasks were taken')
    test:is(list[1][1], id, 'id was not changed')


    id = lp:_last_id() + tonumber(1)

    local fiber_run = false
    local fiber_done = false
    fiber.create(function()
        fiber_run = true
        local list = lp:subscribe(id, 2, 'a', 'b', 'c')
        test:is(#list, 2, 'one task was taken')
        test:is(list[1][1], id, 'id')
        test:is(list[1][4], 'd', 'data')
        fiber_done = true
    end)

    fiber.sleep(0.1)
    test:ok(fiber_run, 'fiber is run')
    lp:push('c', 'd')
    for i = 1, 10 do
        fiber.sleep(0.1)
        if fiber_done then
            break
        end
    end
    test:ok(fiber_done, 'fiber is done')

end)

-- test:diag(yaml.encode(box.space.LP:select()))
-- test:diag(tnt.log())
tnt.finish()
os.exit(test:check() == true and 0 or -1)

