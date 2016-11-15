#!/usr/bin/env tarantool

local PLAN      = 18

local MASTER_PORT = tonumber(os.getenv('MASTER_PORT'))
local REPLICA_PORT = tonumber(os.getenv('REPLICA_PORT'))
local TEST_DIR = os.getenv('TEST_DIR')

local fio = require 'fio'
local log = require 'log'
local dir = fio.pathjoin(TEST_DIR, 'master')

fio.mkdir(dir)

box.cfg {
    listen      = MASTER_PORT,

    wal_dir     = dir,
    snap_dir    = dir,
    vinyl_dir   = dir,

    logger     = fio.pathjoin(dir, 'tarantool.log'),
}

box.schema.user.create('test', { password = 'test' })
box.schema.user.grant('test', 'read,write,execute', 'universe')

package.path = string.format('%s;%s',
    'lua/?.lua;lua/?/init.lua',
    package.path
)

-------------------------------------------------------------------------------
-- TEST
-------------------------------------------------------------------------------

local fiber = require 'fiber'
local net = require 'net.box'
local yaml = require 'yaml'

local test = require('tap').test()
test:plan(PLAN)



local lp = require 'lp'

test:ok(lp:init({ mode = 'master', expire_timeout = 0.5 }) > 0, 'First init lp')
test:ok(box.space.LP, 'Space created')


local replica

for i = 1, 10 do
    replica = net.connect(string.format('%s:%s@localhost:%s', 'test', 'test', REPLICA_PORT))
    if replica:ping() then
        break
    end
    fiber.sleep(0.25)
end

test:ok(replica:ping(), 'replica is up')


log.info('push task1')
test:ok(lp:push('key1', 'value1'), 'push 1 task')

log.info('push task2')
local task = lp:push('key2', 'value2')
test:ok(task, 'push 2 task')

log.info('push task3')
test:ok(lp:push('key3', 'value3'), 'push 3 task')

local started = fiber.time()
list = replica:call('lp:subscribe', 1, 3, 'key2')
test:is(#list, 2, 'one task received from replica')
test:is(list[1][4], 'value2', 'task data')
test:ok(fiber.time() - started < 0.1, 'wakeup fiber by trigger on_replace')


fiber.sleep(0.61)

local list2 = replica:call('lp:subscribe', 1, 0.1, 'key2')
test:is(#list2, 1, 'expired removed task')

test:is(list2[1][1], list2[1][2], 'min_id = last_id')

test:is_deeply(box.space.LP:select(), {}, 'empty space')



test:is(lp:init({ mode = 'master', expire_timeout = 500 }), 0, 'Reinit')

test:is(lp:push_list('key1', 'value1', 'key2', 'value2', 'key3', 'value3'), 3,
    '3 tasks were put')
started = fiber.time()

local list3 = replica:call('lp:subscribe', list2[#list2][2], .1, 'key2')

test:is(#list3, 2, 'one task received from replica')
test:is(list3[1][4], 'value2', 'task data')
test:ok(fiber.time() - started < 0.1, 'wakeup fiber by trigger on_replace')
test:is(list3[1][1], list2[1][1] + 1, 'id')

os.exit(test:check() == true and 0 or -1)

