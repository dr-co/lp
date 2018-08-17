#!/usr/bin/env tarantool

local MASTER_PORT = tonumber(os.getenv('MASTER_PORT'))
local REPLICA_PORT = tonumber(os.getenv('REPLICA_PORT'))
local TEST_DIR = os.getenv('TEST_DIR')

local fio = require 'fio'

local dir = fio.pathjoin(TEST_DIR, 'replica')

fio.mkdir(dir)

box.cfg {
    listen      = REPLICA_PORT,

    wal_dir     = dir,
    memtx_dir    = dir,
    vinyl_dir   = dir,

    replication  = {
        string.format('%s:%s@localhost:%s', 'test', 'test', MASTER_PORT)
    },

    log     = fio.pathjoin(dir, 'tarantool.log'),
}



package.path = string.format('%s;%s',
    'lua/?.lua;lua/?/init.lua',
    package.path
)



_G.lp = require 'lp'

lp:init({ mode = 'replica', lsn_check_interval = .1 })

