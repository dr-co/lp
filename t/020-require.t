#!/usr/bin/env tarantool

local yaml = require 'yaml'
local test = require('tap').test()
test:plan(5)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

package.path = string.format('%s;%s',
    'lua/?.lua;lua/?/init.lua',
    package.path
)

local lp = require 'lp'

test:ok(lp:init() > 0, 'First init lp')
test:like(tnt.log(), 'First start of LP', 'upgrade process started')
test:is(lp:init(), 0, 'Reinit does nothing')
test:ok(box.space.LP, 'Space created')


tnt.finish()
os.exit(test:check() == true and 0 or -1)

