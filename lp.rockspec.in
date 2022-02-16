package = 'megaqueue'
version = '@@VERSION@@'
source  = {
    url    = 'git+https://github.com/dr-co/tarantool-megaqueue',
    branch = 'master',
}
description = {
    summary  = "Queue for Tarantool",
    homepage = 'https://github.com/dr-co/tarantool-megaqueue',
    license  = 'BSD',
}
dependencies = {
    'lua >= 5.1'
}
build = {
    type = 'builtin',

    modules = {
        ['megaqueue'] 			= 'megaqueue/init.lua',
	['megaqueue.migrations'] 	= 'megaqueue/migrations.lua',
	['megaqueue.stats']		= 'megaqueue/stats.lua',
    }
}

-- vim: syntax=lua

