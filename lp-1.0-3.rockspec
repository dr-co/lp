package = 'lp'
version = '1.0-3'
source  = {
    url    = 'git+https://github.com/dr-co/lp',
    branch = 'master',
}
description = {
    summary  = "Event server Tarantool",
    homepage = 'https://github.com/dr-co/lp',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',

    modules = {
        ['lp'] 			= 'lp/init.lua',
	['lp.migrations'] 	= 'lp/migrations.lua',
        ['lp.pack_key']         = 'lp/pack_key.lua',
    }
}

-- vim: syntax=lua

