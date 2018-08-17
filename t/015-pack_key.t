#!/usr/bin/env tarantool

local test = require('tap').test()
test:plan(4)

package.path = string.format('%s;%s',
    'lua/?.lua;lua/?/init.lua',
    package.path
)
local msgpack = require 'msgpack'

local pk = require 'lp.pack_key'


function check_method(m) 
    test:test(m .. ' method', function(test) 
        test:plan(5)

        test:ok(pk[m].pack('abc'), m .. ': pack scalar')
        test:ok(pk[m].pack({'abc'}), m .. ': pack table')
        test:is_deeply(
            pk[m].unpack(pk[m].pack('abc')),
            'abc',
            m .. ': pack/unpack table'
        )

        test:ok(pk[m].pack{ 'abc', msgpack.NULL }, 'pack null')
        if m == 'colon' then
            test:diag(pk[m].pack{'abc', msgpack.NULL})
            test:is_deeply(
                pk[m].unpack(pk[m].pack{'abc', msgpack.NULL}),
                { 'abc', 'null' },
                m .. ': pack/unpack table'
            )
        else
            test:is_deeply(
                pk[m].unpack(pk[m].pack{'abc', msgpack.NULL}),
                { 'abc', msgpack.NULL },
                m .. ': pack/unpack table'
            )
        end
    end)
end

check_method 'msgpack'
check_method 'json'
check_method 'none'
check_method 'colon'


os.exit(test:check() == true and 0 or -1)


