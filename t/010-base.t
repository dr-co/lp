#!/usr/bin/env tarantool

local test = require('tap').test()
test:plan(1)

local tnt = require('t.tnt')
test:ok(tnt, 'tarantool loaded')
tnt.cfg{}

tnt.finish()
os.exit(test:check() == true and 0 or -1)
