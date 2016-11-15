#!/usr/bin/env tarantool

local test = require('tap').test()
test:plan(4)

package.path = string.format('%s;%s',
    'lua/?.lua;lua/?/init.lua',
    package.path
)

local pk = require 'lp.pack_key'


function check_method(m) 
    test:test(m .. ' method', function(test) 
        test:plan(3)

        test:ok(pk[m].pack('abc'), m .. ': pack scalar')
        test:ok(pk[m].pack({'abc'}), m .. ': pack table')
        test:is_deeply(
            pk[m].unpack(pk[m].pack('abc')),
            'abc',
            m .. ': pack/unpack table'
        )
    end)
end

check_method 'msgpack'
check_method 'json'
check_method 'none'
check_method 'colon'


os.exit(test:check() == true and 0 or -1)


